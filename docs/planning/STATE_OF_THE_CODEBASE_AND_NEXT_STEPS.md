# State Of The Codebase And Next Steps

## Reality check — 2026-09-04

This section supersedes the historical assessment below for present-day status.
It applies the complete `reality-check-for-project` workflow: documented goals,
source and shipped behavior, backlog coverage, a bridge plan, ambition rounds,
and repeated Beads refinement. It is an assessment and implementation backlog,
not a claim that the planned engine changes have been implemented.

### Verdict and evidence boundary

FrankenSQLite is a substantial, functioning SQL database implementation. It is
not finished, not established as a fully compatible SQLite replacement, and not
yet delivering its full concurrent-writer, native durability, and distributed
storage vision through production entrypoints. The principal problem is the
distance between sophisticated component implementations and their integration
into the database users actually open. A second problem is evidence that proves
less than its surrounding prose or closed Bead implies.

The source inspection started at `069063ea1076e3944e4e750b33284cc64d183f7e` on
`main`, with pre-existing edits to `connection.rs` and two GH408 probe files.
Peers continued editing that file during the review. Source findings identify
symbols as well as paths; they are not an assertion that a clean exact revision
was tested. No engine source was changed by this assessment.

Current working-tree core SQL proof is **71 passed, 0 failed** after a peer's
lock repair. Both the initial local conformance
attempt and the permitted RCH retry on `vmi1227854` exited 101 before running a
test: Cargo needed to update `Cargo.lock` under `--locked`. The manifest adds
`stacker`, while that initial lock lacked `stacker` and `psm`. The first RCH content-receipt
attempt separately rejected the existing `.doctor/latest` symlink; retrying the
normal remote lane reached Cargo and confirmed the lock failure. Do not relax
`--locked` or count either failure as a product-test result.

A peer then repaired the working lock with stacker 0.1.25 and psm 0.1.32.
The audit detected that change and launched a fresh locked conformance run on
RCH worker `hz4`; it passed dependency resolution and started compiling at
2026-09-04T22:26:03Z. Local HEAD had advanced to metadata commit
`1449aa98cfebd7d9854f6ccc280fe7d78ee81941`, with peer source edits still present.
This is a working-tree run, not clean-commit certification. It finished at
2026-09-04T22:31:16Z with exit 0: 71 passed, 0 failed, 0 ignored, 0 filtered;
test execution took 0.43 seconds after 5m11s compilation. All 71 named test
verdicts were independently attributed despite interleaved logs. The suite
also emitted 76 `drop_close` warnings about missing awaited close, so it does
not prove region quiescence or complete transactional cleanup. That residual
was added to existing async-test migration owner `bd-ybde3`. Workspace-wide
check/clippy/fmt, full conformance and concurrency certification were not run
by this assessment. No engine code was changed by this assessment.

Shipped behavior was tested separately using the official Darwin arm64
**v0.3.16** asset, published 2026-09-04, whose release manifest names source
`90d340d9de1d0121b8715096166abff5d2c6d58a`. The archive SHA-256 matched the
GitHub asset digest
`0fb98eee3a828a270f222611537bb1b6b63144a1b0629efbc773451627efc8c1`.
This checks archive integrity, not the minisign signature. The diagnostic
oracle was Apple's SQLite **3.51.0**, not the canonical parity target 3.52.0;
these probes are not a strict conformance certificate.

- Passed on the released binary: basic CRUD, joins/CTEs/windows, savepoints,
  JSON extraction/table iteration, FTS5 MATCH, R-tree, STRICT integer coercion,
  and real `defer_foreign_keys` behavior. Ordinary tables, FTS5, and UTF-16LE
  text reopened successfully. Stock SQLite read the files and returned
  `integrity_check=ok`.
- Four overlapping CLI processes each committed ten inserts to a disjoint
  table. All forty rows and sums were correct, and stock integrity checking
  passed. Process lifetimes overlapping does **not** prove transaction overlap,
  SSI correctness, many-writer scaling, fairness, or crash safety.
- Reproduced **GH407 / bd-0pkki**: an outer `COUNT(*)` over an `IN` subquery
  containing `GROUP BY ... HAVING count(DISTINCT ...)` returned `NULL` where
  stock returned `1`, both with successful exits. This is a silent wrong result.
- `PRAGMA key` accepted a key, but the file retained the ordinary SQLite header
  and stock SQLite read the plaintext without a key. Encryption is not wired.
- FTS3 and the Geopoly virtual table explicitly failed as unimplemented.
  File-backed historical SQL failed with a missing-snapshot error.

Probe inputs, JSON outcomes, release metadata, hashes, and complete failed
build logs are retained at
`/tmp/frankensqlite-reality-20260904-wpCKDs`. That is a local audit bundle,
not a permanent public artifact. The Beads contain the important reproductions
and acceptance conditions so implementation does not depend on that directory.

### Document authority and intake

AGENTS.md (975 lines) and README.md (3,020 lines) were read completely. The
parallel source-investigation lanes also completed the canonical comprehensive
spec (18,208 lines), its CODEX companion (2,230), both identical spec-difference
documents, all seventeen other planning documents (8,878), all 28 design
documents, both ADRs, concurrency/invariant/recovery contracts, relevant crate
READMEs and proof packs, FTS5 architecture and proof contracts, and worker/SDK
documentation. The release architecture and connection-decomposition plans
were read separately. Historical assurance essays were examined as analysis,
not current test receipts.

The performance negative ledger received its full 636-heading inventory plus
the complete relevant entries; its entire 22,245-line historical body was not
read. This assessment ran no optimization experiment and claims no new
performance measurement. The source investigation traced each goal's runtime
seam; it was not an exhaustive line-by-line audit of all Rust source.

The documents disagree on important requirements. These are defects to repair
in their existing sources, not permission to shrink the goal:

1. Canonical spec §2.4 still prescribes serialized default BEGIN, directly
   violating AGENTS and both live `concurrent_mode_default = true` constructors.
   Preserve concurrent defaults; correct the stale specification.
2. Commit visibility must use committed `CommitSeq`, with a retained floor
   version for old snapshots. Older TxnId/in-flight algorithms are historical.
   Resolve the spec's incompatible page-witness rules before implementing them.
3. RaptorQ's probabilistic decoding cannot support “any K symbols always
   reconstruct.” State rank-failure/slack and corruption-model assumptions.
   TLA skeleton invariants equal to TRUE are not whole-engine checked proofs.
4. “Wait-free modulo RwLock” and instruction-count-derived nanosecond worst-case
   waits are not scheduler-independent progress proofs. A queue model defining
   `L=min(C, lambda*W)` cannot trigger on `L>C`. Repair the actual mathematics.
5. Process death, power loss, unsynced append failure, and durable-but-unpublished
   commits have different outcomes. An un-fsynced write need not disappear on
   process reopen; a durable commit must not be called rolled back just because
   its notification was lost.
6. README's synchronous examples, unavailable `defer_foreign_keys` claim,
   universal aligned/no-copy page claims, and channel-coordinator description
   conflict with current code. The C example confuses ROW (100) and DONE (101).
7. Actions is disabled, verified live on 2026-09-04. Owner decision `bd-0p0sp`
   keeps it off and uses DSR. Workflow membership is not an executed gate;
   several old plans still require auto-publish or merge protection by Actions.

### Vision checklist and gap coverage

`PARTIAL` means some real implementation; `UNPROVEN` means the stated guarantee
has not been demonstrated on the audited source; `INTEGRATION GAP` means useful
components exist without the promised live database path. A passing shipped
smoke is explicitly narrower than full acceptance. Closed primitive work is
retained as useful evidence and is not counted as a completed product feature.

| # | Concrete promised user outcome | Current reality | Existing owner / missing seam |
|---|---|---|---|
| V01 | Ordinary SQL through Rust and CLI | Real; released smoke and 71 working-tree core SQL oracle tests pass; lifecycle warnings remain | `bd-1dp9`, `bd-2yqp6`, `bd-ybde3`; complete reproducibility checks remain |
| V02 | Full SQLite semantic parity | PARTIAL; GH407 wrong result, trigger limits, encoding/collation and WITHOUT ROWID residuals | `bd-0pkki`, `bd-3lj3`, `bd-nc5hv`, `bd-yqjjx` |
| V03 | Concurrent writers enabled by default | Implemented in both constructors and harness defaults; preserve as an invariant | Existing MVCC/concurrency owners; never introduce global write serialization |
| V04 | Page-level conflict handling with serializability and crash safety | Real live FCW/SSI and page ownership; unresolved allocator/orphan/multi-page frontiers | `bd-ioq6x`, `bd-i09t3`, `bd-filebacked-mvcc-planning-frontier-zvwzb`, existing keeper owners |
| V05 | Concurrency evidence measures actual concurrent storage execution | Two named canonical suites run FrankenSQLite sequentially in memory | New selector/observed-overlap keeper under existing certification work |
| V06 | High throughput and bounded tail latency from many writers | UNPROVEN at this source; physical commit awaits under registry guard | `bd-i0tn6`, `bd-1b7j0.1`, `bd-uh1fv`, `bd-kjw76`; profile before changing safe publication |
| V07 | Cross-process MVCC/SSI/GC authority | PARTIAL; live process-local registry plus WAL/lock/certificate mechanisms | `bd-zywqc.20`, `bd-2385c`, `bd-1goc2` |
| V08 | Stock-compatible durable files on supported OSes | Basic macOS reopen works; Windows lock plane real, shared-content mapping still heap-backed | Existing Windows lock receipt plus missing GH395 content-mapping owner |
| V09 | Cross-database ATTACH transactions atomic through crashes | In-process transaction handling exists; crash window remains | `bd-attached-multidb-crash-atomicity-mhbf1` |
| V10 | Structured async work and bounded cancellation | Async public path real; sync/truncate still blocking and coordinator task only awaits shutdown | `bd-2jpu6`, `bd-28z4i.1`, `bd-1b7j0.1`, `bd-33igf` |
| V11 | Effective planner and bounded storage execution | Single-table planner genuinely wired; skip-scan and interpreter/fallback retirement incomplete | `bd-nax2y`, `bd-1dp9.6.7.3/.5/.6` |
| V12 | Complete useful extensions | JSON/FTS5/Rtree real; FTS3, Geopoly vtab, automatic Session integration incomplete | `bd-26rjq`, `bd-9u4zp`; keep feature denominator intact |
| V13 | FTS5 works on large consumer corpora with bounded memory | Lazy MATCH and shadow persistence real; scoring/full scans/writes can hydrate entire corpus | `bd-fts5-lazy-shadow-reads-itcc4.5/.6`, `bd-2nzo8.5.3` |
| V14 | PRAGMA resource controls change real engine behavior | cache_size/mmap_size/temp_store/threads stored but inert | `bd-dwjnq`; split concrete resources and prove effects |
| V15 | Native ECS database through public SQL | INTEGRATION GAP; capsule/record/codec machinery exists, public pager remains compatibility path | `bd-3mgq5` needs granular runtime children |
| V16 | Native crash-durable two-fsync commit and recoverable indexes | `native_commit` toggles booleans, accumulates markers; no actual fsync and SSI revalidation skipped | Reuse closed `bd-15jh` primitives; new durable integration and crash keepers |
| V17 | Durable SSI witnesses and independently replayable commit proofs | Object/model machinery exists; no demonstrated live durable witness plane in both modes | Reuse `bd-3t3.9`, `bd-1if1`; missing production bridge |
| V18 | Ordinary database self-heals using RaptorQ | Optional FEC hooks/components exist; ordinary Connection adapter starts without hook | `bd-ewwia`, `bd-d1tdm`, native integration; require genuine corruption recovery |
| V19 | Authenticated, encrypted pages and durable key rotation | Crypto modules exist; released PRAGMA key silently leaves plaintext | `bd-1vwiy`; real compat/native paths and wrong-key/restart tests |
| V20 | Historical SQL survives reopen, DDL, compaction and retention | History infrastructure exists; file-backed SQL probe fails | `bd-cd7jt`, `bd-acg74`, `bd-yaomh.*`, `bd-1mt2x` |
| V21 | Native replication, quorum durability and snapshot catch-up | Closed transport/model components; missing full source-SQL to durable follower SQL path | Closed `bd-1hi.19`/`bd-2kc4`; new live integration children |
| V22 | Real tiered storage with safe local reclamation | Closed model components; no proven production upload/receipt/evict/refill path | Closed `bd-1hi.29`/`bd-2d3i.3`; `bd-28z4i.6.1` only partial coverage |
| V23 | Durable epochs, authenticated bootstrap and retention-safe native compaction | Component state machines exist, real publication/restart obligations unproved | Reuse epoch/compaction machinery; new native integration children |
| V24 | First-class SQL queues, leases, worker ranges and honest diagnostics | Closed track uses ordinary test-created tables and seeded diagnostic strings | Closed `bd-agent-swarm-coordination-transparency-8jr6u`; missing production registration/storage and telemetry |
| V25 | SLO policy controls real execution safely | Closed pure evaluator/report; Production source and actuator remain future | Closed `bd-swarm-slo-resource-governor-qb256`; new sampler/actuator seams, reuse telemetry/composition owners |
| V26 | Browser persistence, workers, SDK ownership and lifecycle | Promise/worker/memory path real; OPFS/IDB not implemented; transaction/dispose state defects | Existing WASM epics; refine `bd-3mwjr`, `bd-3r8ay`, `bd-odjvh` |
| V27 | Consumable C/Rust interfaces and examples | Simple C facade real; step eagerly materializes rows; Rust/C README examples broken | Simple-use C acceptance was valid; new executable-example keeper, review `bd-2jpu6.6` against async architecture |
| V28 | Honest release and parity certification | DSR release exists; Actions off; one locked SQL target passes, full exact-release proof remains open | `bd-1dp9.9.8`, `bd-zywqc.9`, `bd-wwqen.8`, DSR canary owner |
| V29 | Byte-bounded memory, snapshot age, and mathematically justified progress | PARTIAL/UNPROVEN across live subsystems; stale proof claims cannot certify budgets | Existing trigger/resource work; verify complete live budget coverage and repair proofs |
| V30 | Physical multi-WAL shards with atomic cross-shard decisions | Parallel buffer/segment machinery exists; ordinary adapter still has one WAL path | Current parallel-WAL owners plus staged `bd-6hdwo.23/.24` |
| V31 | Durable consensus with explicit linearizable reads | No production election/log-agreement/read-consistency protocol found | Staged `bd-6hdwo.25/.26`, after real replication/snapshots |
| V32 | GPU acceleration of real bulk RaptorQ work | No GPU provider found; CPU codec is real | Staged `bd-6hdwo.27/.28`, measured hardware admission and native snapshot consumer |
| V33 | PMEM-native durability without ordinary WAL | No PMEM VFS/persistence-domain path found | Staged `bd-6hdwo.29/.30`; actual hardware proof required |
| V34 | Public vectorized analytical SQL and parallel workers | Operator modules real but not live SQL dispatch; batch MakeRecord is a real narrower optimization | Refined `bd-b434d`, companion `bd-6hdwo.33` |
| V35 | Persistent automatically materialized compressed column groups | In-memory cracking/batches exist, persistent dual-store integration absent | Staged `bd-6hdwo.31/.32` |

The starting Beads snapshot contained **3,851** issues: 3,392 closed, 380 open,
62 in progress, 2 explicitly blocked, 11 deferred, and 4 batch-pending.
Derived blocked-by-dependency counts overlap those statuses. The closed share
is not a percentage of product completion.

Implementing every previously open/in-progress Bead would not, by itself,
close the entire vision. Major SQL/browser/performance work is already covered,
but several live-integration requirements are represented only by closed
component tasks. Native work has coarse coverage without sufficient executable
decomposition. Some open acceptance criteria are stale or permit simulated
evidence, and an epic was incorrectly a blocking prerequisite of its own
leaves. Refining that graph is part of this assessment, not an excuse to
replace the backlog or reopen every historical component.

### Bridge plan: deliver user-visible capabilities with separate proof

**First restore a trustworthy execution baseline.** Resolve manifest/lock
closure through the approved Cargo route, retain the current toolchain, and
run the exact source core SQL oracle plus required workspace check/clippy/fmt.
Keep the failing GH407 SQL in its existing owner. Bind canonical concurrency
selectors to actual file-backed overlap and reject falsely labeled sequential
receipts. Correct authority contradictions and execute consumer examples.

**Stabilize compatibility correctness before performance surgery.** Existing
P0 allocator, orphaned-page, multi-page update, busy-recovery and attached-DB
owners retain their scopes. `bd-9inpb` is closed; its successful allocator
repair does not close `bd-ioq6x`. Finish the live cross-process authority and
Windows content mapping with real stock interoperability. Channel unification
and moving physical I/O out of the registry must preserve allocation ownership,
durable sequence reservation, publication order, cancellation and retry safety.
Do not shorten the lock by dropping its current correctness protection.

**Complete SQL and resource behavior in existing tracks.** Retire specific
fallbacks only after their oracle coverage exists; preserve the real planner
integration already landed. Extend FTS5 acceptance to ranked/snippet queries,
small writes, rollback and reopen on the actual large corpus, measuring RSS and
hydration. Wire resource PRAGMAs to real budgets. Finish heap trigger frames,
extension functionality, browser storage and SDK transaction/lifecycle rules.
Do not use nominal API presence, fake workers, or quick small-corpus MATCH as
proof of those goals.

**Build native mode as a vertical database path.** Reuse existing codecs and
models, then add: durable authenticated object-store bootstrap; real two-fsync
commit/recovery; a SQL pager that persists schema/pages/indexes; durable SSI
witnesses and replayable proofs in both modes; streaming multi-block snapshots;
retention-safe compaction; epoch/key transitions; genuine follower apply and
quorum receipts; and real remote tier upload/refill/eviction. Each implementation
has a separate test Bead. Encryption and historical SQL keep their canonical
owners and gain mode-specific runtime acceptance. Compatibility stays available
and concurrent writers stay enabled by default in both modes.

**Finish closed features at their missing boundaries.** SQL coordination must
register real queue/lease/range surfaces on a fresh Connection without test
CREATE TABLE substitutes. Diagnostics must come from executed statements and
actual conflicts/fallbacks. The SLO governor must first receive real shadow
signals, then act at bounded cancel-aware admission/helper/checkpoint seams,
with a verified kill switch. Reuse the evaluator and contracts; do not build
another report/schema framework in place of the missing engine integration.

**Qualify the exact release through DSR/local/RCH execution.** Run semantic,
concurrency, crash, corruption and resource lanes separately; bind every result
to revision, dirty-source manifest, platform, toolchain, feature set, command,
profile, seed, oracle version, outcomes and artifact hashes. A skipped build,
zero tests, unsupported mode, both-engines-error result or model-only test must
not become a pass. Preserve the full canonical feature denominator. Performance
claims require the same-source representative matrix, correct release-perf and
portable-versus-native provenance, quiet-host authorization where needed, and
the negative-results ledger before experimentation.

### Shared implementation and test contract

Every new implementation Bead states its runtime seam, prerequisite capability,
error/cancellation behavior and observable user outcome. Every companion test
Bead covers positive, boundary, failure and adversarial cases, including a
negative control that fails when the intended path is bypassed. Storage tests
use actual public Connection, pager and WAL/native files; process-kill tests
use sacrificial test-owned database copies. No user or peer files may be deleted.

Use existing crate test directories and runner infrastructure, never inert
workspace-root tests. For relevant `bd-zywqc` fixes, enumerate all nine taxonomy
categories or a permitted explicit exemption. Log with the canonical E2E schema
(`run_id`, UTC timestamp, phase, event_type, scenario/seed and context paths),
adding transaction/process/connection IDs, mode, observed overlap, commit and
durability boundaries where applicable. Never log keys or plaintext secrets.
Each closure names commands, revision, platform, actual test records, exit
codes, first divergence and reproducible artifacts. Actions stays off.

### Iteration record

**Ambition 1 — replace native feature labels with a durable database sequence.**
The initial bridge was too coarse about native storage. The following bounded
capabilities preserve all native promises and make their prerequisites explicit.
Every row receives a separate implementation and companion proof Bead; existing
encryption/history owners retain their identities.

| Node | Runtime deliverable | Prerequisite capabilities | Distinguishing acceptance |
|---|---|---|---|
| N0 | Correct snapshot ESI, genuine repair decoding, fail-closed codec errors | Existing codec | Permanently erase source ESI 0; reconstruct exact bytes from real repair symbols; no success-seed search |
| N1 | Durable SymbolStore and authenticated native root bootstrap | Existing envelopes/logs | Actual native files reopen in another process; torn roots and wrong identities fail closed |
| N4 | Durable witnesses and replayable SSI proof, both modes | N1 and live SSI | Recompute write-skew/phantom verdict from persisted evidence after process restart |
| N2 | Real capsule/fsync1/marker/fsync2/publication/ACK | N1, N4 | Kill at every boundary; every acknowledged transaction survives; no phantom commit |
| N3 | Public native SQL pager, schema, version and index storage | N2 | CRUD/DDL/index/transaction oracle after reopen; repair/rebuild indexes from committed history |
| N5 | Consistent resumable multi-block checkpoint/snapshot | N0, N3, N4 | Transfer during writes with loss/reordering; second database agrees at one committed boundary |
| N6 | Durable native compaction and retention | N3, N4, N5 | Cross-process old snapshots stay readable; kill/cancel every publish phase; measure real reclaimed bytes |
| N7 | Authenticated epoch/policy rotation | N1, N2; native SQL for final keeper | Reject forged/future epochs and old-policy new publication; retain authenticated historical epochs |
| N8 | Real follower apply, transport and durable remote quorum | N0, N3, N4, N7 | Follower SQL survives restart; pre-fsync ACK cannot count; duplicate/unauthorized identities cannot satisfy quorum |
| N9 | Remote tier upload, verified receipt, safe eviction and refill | N6, N7; shared durable receipt contract | Interrupted upload retains local safety; historical reads refill real remote bytes after restart |
| N10 | Real page encryption and key rotation | Compat path independent; native N3/N7 | Missing/wrong key fails after reopen; ciphertext repair and crash-safe KEK rewrap of the stable DEK |
| N11 | Historical schema/page/index SQL | Native N3/N6; existing compatibility history path | DDL/drop/index changes, timestamp edges and retention races resolve to historical contents |
| N12 | Same public SQL corpus in actual compat and native modes | N3 | A native label over memory/compat storage is rejected; results remain separated by mode |

This decomposition exposed concrete defects before any network integration:
`snapshot_shipping` passes `isi-k` to the locked asupersync codec that requires
ESI >= K; both receive paths currently reconstruct only when all source symbols
arrive; encoding failure and WASM paths can emit deterministic fake repair
packets. Existing tests can call a run repaired merely because a repair packet
was seen before every source packet eventually arrived. These are source
deductions, not executed reproductions. N0 must fix the real codec call and
decoder integration, with permanent erasure tests, before N5/N8 can qualify.

N4 precedes N2: witness persistence integrates with the existing live SSI seam,
so proof publication does not create a circular native-commit dependency.
Existing component implementations and closure history remain credited where
supported. Each original acceptance contract is assessed separately; closed
status alone does not establish runtime integration.

**Ambition 2 — test the consumer operation that defeats the abstraction.**
The first native pass still allowed a component to report success while the
user operation failed. Add these concrete adversarial acceptance boundaries:

- SDK callback transaction ownership: pause a callback after writing A, issue
  unrelated database write B, then roll back A. B must be explicitly refused or
  execute outside A; a successful B must not disappear. Escaped transaction
  handles reject after completion. Preserve callback errors if rollback fails.
- SDK disposal: every in-flight request settles exactly once on close/crash,
  later database and prepared-handle requests fail promptly, and repeated close
  has a defined outcome. Current `pending.clear()` can abandon promises.
- FTS5: cold-open a large index, run MATCH with BM25/snippet, make a small write,
  roll it back, and reopen. Record hydrated rows and peak RSS throughout, not
  just lazy-open or small MATCH success. Preserve old real-corpus owners.
- Encryption: split compatibility and native implementation prerequisites so
  a usable compatibility encryption path is not blocked on the whole native
  architecture. Missing-key reopen, authenticated metadata, nonce uniqueness,
  key rotation and ciphertext repair are independent required outcomes.
- Native history: bind historical schema, roots and indexes together. Test
  DROP/ALTER/index changes between snapshots, not only updates in one schema.
  Retained history must survive compaction and remote refill; expired history
  must fail explicitly rather than silently returning current rows.
- Replication: permanently remove source symbols, use fixed loss schedules,
  and require durable follower SQL after restart. Seeing a repair packet or
  counting caller-provided store IDs cannot prove decoding or remote durability.
- SLO enforcement: disconnect the actuator while leaving the evaluator/report
  intact; the enforcement test must fail. Control actions must preserve SQL
  correctness, bounded cleanup and concurrent defaults under composition.
- A complete release run must actually execute every selected target. Bind
  compiler/test outcomes and mode identities, reject zero-test/skip/paired-error
  shortcuts, and retain the full feature denominator. Add these requirements
  to existing certification/orchestrator owners instead of a new framework.

**Ambition 3 — use mathematical structure to reduce uncertainty and work.**
Additional sophistication is useful only if it improves an actual acceptance
decision. Reuse existing mechanisms with these operational proof obligations:

1. **Conflict histories and partial-order exploration.** Trace real read/write,
   validation, durable publication and cancellation events; construct the
   dependency graph and check forbidden cycles against recorded SQL outcomes.
   Feed the same live seams into existing LabRuntime/DPOR work. Explore
   independent-event equivalence classes instead of multiplying arbitrary
   thread schedules. The independence relation itself needs counterexamples
   involving allocation, page reuse, schema changes and witness retention.
   A scheduler model or a TLA skeleton is explicitly narrower than this proof.
2. **Durability as a state machine with a recoverable decision.** Define the
   linearization and durable-decision points, enumerate crash/cancel cuts,
   and require replay to reach the same terminal result. A lost ACK is an
   uncertain completion requiring idempotent reconciliation, not automatic
   rollback. Apply this to attached databases, native commit, key rotation,
   compaction and remote receipts. Do not confuse process death with power loss.
3. **Byte conservation and bounded retention.** Every variable subsystem must
   identify who reserves bytes, who releases them, and what happens at a limit.
   Cover pages/deltas, writes, witnesses, pins, indexes, symbols, sort/aggregate
   state, trigger frames, request queues and buffers. Counters cannot be mere
   reported estimates while allocations grow unbounded. Enforce live snapshot
   age with the caller's monotonic budget and typed error; the standalone
   TransactionManager's timeout test does not prove Connection enforcement.
4. **Conditional performance models, measured on their assumptions.** Separate
   offered load, admitted load, service time and queue depth; avoid a clamped
   variable that makes overload detection impossible. Tail-latency claims need
   measured arrival/service distributions and explicit scheduler assumptions.
   Existing e-process/conformal machinery may monitor or prioritize evidence;
   it cannot guarantee every safety predicate or predict an alarm before loss.
5. **Decode and remote-durability uncertainty.** Include rank deficiency,
   correlated losses, authentication failures, block identity and distinct
   durable stores. Measure actual decoder rank/contribution, retain sufficient
   local data until authenticated durable receipts meet policy, and fail closed
   when assumptions cannot be established. No unconditional “any K” theorem.
6. **Preserve distant goals without letting them hide current blockers.** The
   §21 WAL-multiplexing, consensus, GPU, PMEM, vector execution and column-store
   horizons remain explicit targets. Closed future-work compliance reports do
   not implement them. Map them to existing runtime owners or staged follow-up
   tasks with admission evidence, dependencies and honest platform boundaries.
   They do not block compatibility correctness or justify hardware/fleet work
   without its required authorization.

These refinements are acceptance and implementation requirements, not fresh
claims of mathematical verification or measured speed. The resulting Beads
must preserve the actual user goals even when a proposed mechanism fails its
correctness or keep gate.

**Refinement 1 — lifecycle, semantic prerequisites and honest terminal scope.**
Reviewed every newly created Bead and the modified existing SQL/SDK/FTS/native
contracts. Converted implementation/test pairs to co-ready related work with
their actual shared prerequisites; implementation closure requires the
independent keeper, while the keeper can run against code still in progress.
This removes a lifecycle deadlock that an acyclic graph alone did not detect.
Populated structured acceptance criteria, corrected native/vector/SDK umbrella
blocking edges, and tailored pure-doc/lock checks to avoid artificial tests.

Native bootstrap proof now starts with compatibility SQL and actual durable
witness-store operations, then requires full native SQL replay downstream.
Added explicit protection of unpublished writer objects during compaction,
directory-sync crash cuts, progressive read-only snapshots, real page/capsule
corruption recovery, retained historical epochs and complete encrypted-copy
coverage. Split pre-publication object durability receipts from post-marker
follower apply (`bd-3mgq5.23/.24`), avoiding a quorum/publication deadlock.
The later combined keeper (`bd-3mgq5.25`) proves history, encryption, compaction
and remote refill together. A focused SDK keeper (`bd-6hdwo.34`) can run without
waiting for unrelated streaming/pooling work. FTS/resource acceptance now
addresses shared-cache arbitration, realistic posting work, isolated corpus
mutation and resource-policy transitions. Distant PMEM/column-store milestones
cannot close before their full promised behavior is delivered.

**Refinement 2 — eliminate proof prerequisites that depend on their consumers.**
Read the complete storage/native/resource contracts again and validated the
full typed graph plus conceptual implementation-to-companion closure edges.
Snapshot foundations now prove real durable pin/resume behavior; the later
compactor and combined keeper prove transfer during compaction. Receipt
foundations prove actual durable objects and local publication admission;
downstream replication proves distinct follower-apply ACK and SQL. This removes
two semantic cycles without reversing the correct capability dependencies.
Completed missing descriptions/structured criteria, linked canonical owners,
and separated SDK lifecycle/transaction subtargets so the base SDK can unblock
its transaction extension. Actual `br` validation found no missing references
or augmented pair-closure cycles. `bv`'s cycle computation is skipped at this
graph size, so its generic DAG advisory is not accepted as proof.

### Historical assessment below

The remainder preserves the earlier performance investigation and rejected
experiments. Its references to “current,” active blockers, and source state
describe its original March-era context, not the September assessment above.

---

This document is a self-contained handoff for external review. It summarizes what FrankenSQLite is trying to do, where the largest performance gaps versus stock SQLite remain, what has already been tried, what worked, what failed, and what looks most promising next.

## Direct Answers Up Front

- Yes, the recent structural preclaim experiment has now been manually reverted from the working tree. The failed benchmark artifacts remain useful historical evidence, but they no longer describe the current code state.
- The benchmark evidence says the structural preclaim idea was a **bad idea** in the forms tried so far. It made the key `commutative_inserts_disjoint_keys / frankensqlite / c8` row much worse, not better.
- I no longer have high confidence in the "acquire shared structural pages earlier" intuition. The evidence now suggests the real issue is not simply *when* contention is detected, but the fact that the shared structural pages become a convoy point at all.
- The revert was done surgically rather than with a destructive git checkout, because the repo still has a dirty working tree with other agents' edits.
- MCP Agent Mail was unavailable during this revert session because the server kept timing out, so coordination could not be completed there even though that remains the intended workflow.

## What This Project Is

FrankenSQLite is an independent ground-up Rust reimplementation of SQLite. The core point of the project is not merely "SQLite in Rust." The core point is to preserve SQLite-like behavior and compatibility while removing SQLite's biggest concurrency bottleneck: the fact that stock SQLite effectively allows only one writer at a time.

At a high level, stock SQLite is a compact, highly optimized embedded SQL engine built around:

- SQL parsing and planning
- a bytecode virtual machine
- a pager and page cache
- SQLite-format B-tree pages for tables and indexes
- a WAL or rollback-journal persistence model

That design is extremely fast on the uncontended single-writer path. It is one of the most performance-tuned software systems in existence. The tradeoff is that write concurrency is intentionally limited. In normal WAL mode, one writer owns the write lock at a time, and everyone else either waits or retries.

FrankenSQLite keeps much of the same broad database shape:

- SQL text is still parsed and compiled into a VDBE-style execution path
- storage is still page-oriented
- tables and indexes are still represented through SQLite-style B-tree structures
- compatibility with ordinary SQLite database files remains a central current-runtime goal

The big difference is that FrankenSQLite tries to replace SQLite's serialized write regime with page-level MVCC so that multiple writers can make progress concurrently when they truly touch different pages.

## What FrankenSQLite Adds Beyond Stock SQLite

The primary new capabilities and design goals are:

### 1. Concurrent writers by default

In stock SQLite, one writer owns the write path. In FrankenSQLite, the design goal is that `BEGIN` effectively behaves like concurrent mode by default, so writers only conflict when they touch the same physical pages or trip serializability rules.

The purpose of this is straightforward:

- raise write throughput on multi-core machines
- avoid a global writer bottleneck
- let independent writers proceed without queueing behind one another

### 2. Serializable snapshot isolation for the concurrent path

The project does not want "faster but weaker" concurrency. The goal is still correct transactional behavior. So the concurrent path layers serializable/snapshot-style validation over page-level write ownership rather than simply letting conflicting writers race.

The purpose of this is:

- preserve correctness
- prevent subtle anomalies such as write skew
- give concurrent writers stronger semantics than "best effort parallelism"

### 3. A safe Rust engine core

The project is also a clean-room Rust engine effort, with most of the engine in safe Rust. That is important for maintainability and correctness, but it is not the benchmark target by itself. Safe Rust is not the reason to exist; concurrent-writer performance is.

### 4. A broader long-term architecture vision

The repo and README also describe larger ambitions such as self-healing durability and native storage modes. Those matter to the long-term project story, but they are not the main thing being benchmarked in the current SQLite-comparison matrix. The performance question in this report is mainly about the live compatibility/pager-backed runtime.

## What Is Actually Being Benchmarked Right Now

This matters because the report needs to be explicit about the current runtime shape rather than the aspirational architecture.

The current benchmark matrix is primarily exercising the live compatibility-oriented path:

- the SQL frontend
- the VDBE/compiler and execution machinery
- the pager-backed storage path
- the MVCC/write-coordination machinery
- the SQLite-style B-tree/table/index layer

In other words, the benchmark is not testing a hypothetical future native object store. It is testing the real current engine path that is supposed to compete with SQLite today.

That means the relevant questions are not abstract. They are:

- how expensive is the current write path versus SQLite's write path?
- how much extra coordination work is FrankenSQLite doing?
- when SQL operations look disjoint, are they actually disjoint in terms of physical pages and B-tree structure?
- how much baseline overhead has the Rust engine accumulated even before concurrency helps?

## Runtime Architecture Walkthrough For The Benchmarked Path

The most useful mental model for the current benchmarked runtime is a top-to-bottom walkthrough of the live path.

### 1. Connection / orchestration layer

This is the layer that owns:

- transaction mode decisions
- connection state
- schema/bootstrap handling
- statement dispatch
- the handoff into compilation and execution

At a high level, this layer decides whether a statement runs in the ordinary path, the concurrent path, or a compatibility/fallback-oriented path. It also owns enough transaction state that a bug here can either corrupt measurements or make the whole engine look slower than it really is. That is why the commit-planning regression fix mattered: it restored correctness, but it did not materially change the performance frontier.

### 2. SQL frontend

This layer turns SQL text into executable work:

- parse the SQL
- resolve names and tables/indexes
- decide execution shape
- prepare the execution program

The benchmark matrix especially exposes this layer in:

- all `c1` rows
- all `mixed_read_write` rows

If this layer is too expensive, even perfect storage concurrency cannot save the overall benchmark.

### 3. VDBE / execution engine

This is the bytecode-style execution core that steps through the compiled program, drives cursors, and issues reads/writes into the storage stack.

At a high level it is where:

- loops run
- rows are decoded
- expressions are evaluated
- storage cursors are advanced
- page writes are ultimately triggered

This layer sits at the boundary between "SQL engine cost" and "storage engine cost." In mixed workloads, it is one of the main places where broad baseline inefficiency can accumulate.

### 4. Cursor and B-tree layer

This is where logical table/index work becomes physical page work. It is responsible for:

- locating keys/rows in tree structure
- reading and writing cells
- handling leaf overflow
- choosing split/rebalance behavior
- updating parent/divider structure

This layer is central to the flagship failure because the benchmark may say "disjoint inserts," but the B-tree may still force those inserts through shared structural pages.

### 5. Pager / VFS / page-buffer layer

This layer manages:

- page fetch and page cache interaction
- reads from backing storage
- write staging and page normalization
- page buffer ownership and copying
- read/write calls into the underlying file abstraction

This layer is especially implicated by mixed workload profiles showing `IoUringFile::read`, `malloc`, and `memmove`. Even if MVCC were perfect, excessive cost here would still leave the engine trailing SQLite.

### 6. MVCC coordination layer

This is the project's core differentiator. It is responsible for:

- tracking staged page versions for the current transaction
- enforcing page ownership / conflict rules
- validating snapshot and commit sequence assumptions
- deciding whether a write can proceed
- restoring or cleaning up state on failed concurrent attempts

This is where the engine is trying to win against SQLite, but it is also where the worst hot-path costs are currently concentrated.

### 7. Allocation / metadata / commit-publication layer

This layer includes:

- allocator-visible page changes
- page-one/freelist effects
- pending commit page surface construction
- publication sequencing and commit-index updates

This is where independent logical work can become coupled again. Even if the main leaf writes are separate, shared allocator or publication surfaces can still turn the workload back into a choke point.

### 8. Benchmark harness layer

The matrix is not just a storage benchmark; it is a workload harness that:

- opens real databases
- drives the same kinds of operations repeatedly
- changes concurrency systematically
- records retries, aborts, and throughput

That is why the matrix is so valuable. It does not just say "the engine is slow." It helps identify whether the loss is:

- baseline and constant-factor
- retry-amplified
- structural
- fixture-shape-sensitive
- or all of the above

## High-Level Implementation Strategy

At a high level, FrankenSQLite tries to stay near SQLite's storage model while changing the write-concurrency model.

The current live design can be described this way:

- SQL is parsed, planned/compiled, and executed through the engine stack rather than delegated to SQLite.
- Storage is page-based, not row-MVCC-based.
- Writers stage page changes inside transaction-local structures.
- Concurrent writer coordination is done around physical pages, not whole connections or whole tables.
- Conflict detection, snapshot validation, and commit/publication machinery decide whether a transaction can publish its page updates.
- B-tree operations still perform structural work such as leaf splits, parent updates, and allocator interactions on SQLite-style pages.

This is the key design tension in the whole project:

- page-level MVCC is attractive because it keeps file-format and storage-model continuity with SQLite
- but page-level MVCC means physical shared pages become the real conflict surface
- so any shared metadata page, parent page, right-edge page, root page, or allocator page can destroy the "writers are independent" story even when the SQL statements look independent

That tension is at the center of the current performance disaster.

## Why This Architecture Can Beat SQLite In Principle

The optimistic case is real.

If two writers genuinely touch different pages, then FrankenSQLite should be able to let both proceed concurrently while stock SQLite serializes them through its single-writer regime. The `hot_page_contention` family shows that this is not just theoretical: on some high-concurrency rows, MVCC does approach or beat SQLite.

So the concurrent-writer thesis is not imaginary. It is just not being realized broadly enough, and it is being crushed by other costs.

## Why This Same Architecture Can Also Lose Badly

FrankenSQLite is trying to buy concurrency by adding machinery that stock SQLite largely does not need on its uncontended path.

At a high level, that machinery includes:

- transaction-local staged page state
- page ownership/lock bookkeeping
- snapshot and serializability validation
- commit-index and publish coordination
- restore/rollback handling for failed concurrent writes
- extra logic around allocator/page-one and structural B-tree surfaces

That creates two broad ways to lose:

### 1. Baseline engine overhead

Even before concurrency matters, FrankenSQLite may simply do more work per operation than SQLite. That includes frontend work, page copying/staging, buffer normalization, bookkeeping, and extra storage-path logic.

If this overhead is too large, FrankenSQLite loses even at low concurrency and even in single-writer mode.

### 2. Conflict amplification

Even when SQL operations seem independent, the engine may force them to converge on shared physical structures:

- page 1
- allocator/freelist metadata
- shared parent/internal B-tree pages
- right-edge growth paths
- commit/publication bottlenecks

When that happens, the project does not just fail to beat SQLite. It can become much worse than its own single-writer mode, because it pays both the baseline overhead and the failed-concurrency overhead.

## Project Goal

FrankenSQLite exists to beat stock SQLite on concurrent writes by replacing SQLite's single-writer serialization with MVCC page-level versioning. The project goal is:

- Match or beat SQLite on normal workloads.
- Beat SQLite clearly on highly concurrent writes.
- Preserve correctness and SQLite-like semantics.

The central failure right now is that FrankenSQLite is still much slower than SQLite on most benchmark rows, and even the "disjoint concurrent inserts" showcase row remains unstable and generally behind.

## How The Project Design Relates To The Performance Gaps

The benchmark matrix makes much more sense once the above architecture is kept in mind.

### Why single-writer performance matters so much

FrankenSQLite is not only losing on MVCC rows. It is also losing badly in its own single-writer lane. That means a large part of the gap has nothing to do with successful or failed concurrent writers. It means the current engine stack is still doing too much work relative to SQLite's extremely optimized baseline.

### Why the "disjoint inserts" row is so revealing

`commutative_inserts_disjoint_keys` is supposed to showcase the value of page-level MVCC. If independent inserts still collide there, it means one of two things is happening:

- the engine's notion of "shared page" is much broader than the SQL workload suggests
- or the concurrent machinery is so expensive that even a partly-successful disjoint path still loses

In practice, the evidence says both are happening.

### Why "hot page contention" is not the whole story

`hot_page_contention` is the family where the concurrent-writer idea actually looks plausible. But even there, the gains only show up clearly at high concurrency, and the baseline still trails SQLite badly at low concurrency. So the project has not merely failed to fix one lock. It has accumulated a system-wide cost problem plus a structural-conflict problem.

### Why compatibility with SQLite pages cuts both ways

Staying close to SQLite's page/B-tree format is useful because it preserves compatibility and keeps the engine conceptually aligned with SQLite. But it also means the engine inherits real structural hot spots:

- parents must still be updated
- leaves still split
- allocator metadata still exists
- root/right-edge effects still exist

So if the implementation is not extremely careful, page-level MVCC ends up being "concurrent writers, except on all the pages that matter most during growth and balancing."

## Current Dirty Working Tree Reality

The repo is currently dirty in many files, including work by other agents. The most relevant files for the current performance discussion are:

- [balance.rs](/data/projects/frankensqlite/crates/fsqlite-btree/src/balance.rs)
- [cursor.rs](/data/projects/frankensqlite/crates/fsqlite-btree/src/cursor.rs)
- [engine.rs](/data/projects/frankensqlite/crates/fsqlite-vdbe/src/engine.rs)
- [connection.rs](/data/projects/frankensqlite/crates/fsqlite-core/src/connection.rs)

The current recent performance-related edits that are definitely mine are concentrated in:

- [balance.rs](/data/projects/frankensqlite/crates/fsqlite-btree/src/balance.rs)
- [engine.rs](/data/projects/frankensqlite/crates/fsqlite-vdbe/src/engine.rs)
- [connection.rs:8873](/data/projects/frankensqlite/crates/fsqlite-core/src/connection.rs:8873)
- [connection.rs:12952](/data/projects/frankensqlite/crates/fsqlite-core/src/connection.rs:12952)
- [connection.rs:13021](/data/projects/frankensqlite/crates/fsqlite-core/src/connection.rs:13021)
- [connection.rs:13248](/data/projects/frankensqlite/crates/fsqlite-core/src/connection.rs:13248)
- [connection.rs:13262](/data/projects/frankensqlite/crates/fsqlite-core/src/connection.rs:13262)

## Benchmark Facts That Matter

### Whole-suite baseline

The last completed whole-suite matrix before the recent structural-preclaim experiments is:

- [sqlite_plus_mvcc_nowarm_repeat1.jsonl](/data/projects/frankensqlite/artifacts/perf/20260313_canonical_baseline/sqlite_plus_mvcc_nowarm_repeat1.jsonl)
- [sqlite_plus_mvcc_nowarm_repeat1.md](/data/projects/frankensqlite/artifacts/perf/20260313_canonical_baseline/sqlite_plus_mvcc_nowarm_repeat1.md)

That run showed:

- MVCC beat SQLite in only `2/27` rows.
- Single-writer beat SQLite in `0/27` rows.
- Median MVCC throughput was about `0.133x` SQLite.
- Median single-writer throughput was about `0.134x` SQLite.

### Matrix legend: what each cell is really exercising

Before listing the full 27-row matrix, it helps to define the subsystem shorthand used below.

Subsystem shorthand:

- `SQL` = parser, name resolution, compilation, VDBE setup/teardown
- `READ` = row decode, cursor reads, read-side execution/materialization
- `IO` = pager, VFS, page fetch/write, page-buffer normalization and copying
- `MVCC` = page ownership, staged page state, snapshot validation, write coordination
- `BT` = B-tree insert/delete/navigation, leaf split, parent update, rebalance
- `ALLOC` = allocator, freelist, page-one metadata, pending commit page surface
- `RETRY` = lock wait path, wake path, backoff, retry/abort amplification
- `PUB` = commit/publication sequencing, commit index, centralized finalization surfaces

High-level design of each subsystem that appears in the matrix:

- `SQL`
  - This is the front half of the engine: parse SQL text, resolve names, build/choose execution shape, and prepare the VDBE/program state that will run the statement.
  - It matters most in low-concurrency rows and mixed workloads, because fixed per-statement cost shows up there immediately.

- `READ`
  - This is the read-side execution path after compilation: cursor navigation, record decode, row materialization, and returning values to the executor.
  - It matters most in `mixed_read_write`, where the engine is doing more than just staging writes.

- `IO`
  - This is the compatibility-oriented storage surface: pager, VFS file operations, page cache interaction, page buffer normalization, and copying data into or out of page-aligned structures.
  - It matters in every row, because every real read or write eventually becomes page movement.

- `MVCC`
  - This is the project's core innovation layer: staged page ownership, snapshot rules, page-level conflict tracking, and the machinery that lets multiple writers attempt work concurrently.
  - It should be the source of the project's upside, but it is also one of the largest current cost centers.

- `BT`
  - This is the SQLite-style B-tree layer for tables and indexes: leaf edits, split decisions, divider handling, parent updates, and rebalance logic.
  - It is especially important in insert-heavy workloads, because logical row operations ultimately become structural page operations here.

- `ALLOC`
  - This is the page allocation/free surface: freelist effects, page-one metadata, and whatever allocator-visible pages must be part of the transaction's commit surface.
  - It matters because logically disjoint inserts can still collide if they all need the same metadata updates.

- `RETRY`
  - This is the dynamic conflict-response system: waiting, waking, backoff, and re-entering the write path after contention.
  - It matters most when the engine is already doing something too expensive, because it multiplies that cost.

- `PUB`
  - This is the finalization layer: commit index updates, publication sequencing, and other centralized surfaces that decide when staged work becomes visible.
  - It matters because even a very concurrent execution path can still bottleneck at publication if this layer is too centralized or too expensive.

Workload design at a high level:

- `commutative_inserts_disjoint_keys`
  - intended to stress concurrent inserts that should mostly stay disjoint
  - should primarily reward good `MVCC`, `BT`, and `ALLOC` behavior
  - if this collapses, the engine is probably manufacturing false sharing or paying too much per-page coordination cost

- `hot_page_contention`
  - intentionally drives writers into overlapping hot pages
  - primarily stresses `MVCC`, `RETRY`, and `PUB`
  - this is where a good concurrent-writer design should at least stay competitive when contention is unavoidable

- `mixed_read_write`
  - stresses the whole engine at once
  - combines `SQL`, `READ`, `IO`, `MVCC`, `BT`, and `PUB`
  - this is the best family for spotting broad baseline inefficiency rather than only write-conflict pathologies

Concurrency tier design at a high level:

- `c1`
  - almost pure baseline cost; minimal true concurrency benefit available
  - bad `c1` numbers usually mean the engine is just expensive

- `c4`
  - transitional regime
  - starts to expose coordination costs and conflict geometry
  - often the best place to see whether concurrency begins helping before outright collapse

- `c8`
  - amplifies every shared-surface mistake
  - if the architecture has hidden serialization or retry storms, this tier exposes it

### Full 27-row matrix with retries and subsystem interpretation

All numbers below come from the complete baseline artifacts above. `SW%` means FrankenSQLite single-writer as a percent of SQLite throughput. `MVCC%` means FrankenSQLite MVCC as a percent of SQLite throughput.

#### Fixture: `frankensqlite`

| Workload | C | SQLite ops/s | SW ops/s | MVCC ops/s | SW% | MVCC% | SW retries | MVCC retries | Subsystems stressed | High-level signature |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|
| commutative_inserts_disjoint_keys | 1 | 6193 | 717 | 807 | 12 | 13 | 0 | 0 | SQL IO MVCC BT ALLOC PUB | baseline cost already huge before contention matters |
| commutative_inserts_disjoint_keys | 4 | 5391 | 545 | 1021 | 10 | 19 | 40 | 24 | SQL IO MVCC BT ALLOC RETRY PUB | MVCC helps relative to SW, but still far behind SQLite |
| commutative_inserts_disjoint_keys | 8 | 2960 | 527 | 74 | 18 | 3 | 89 | 101 | IO MVCC BT ALLOC RETRY PUB | catastrophic false-sharing/retry collapse in flagship row |
| hot_page_contention | 1 | 6598 | 826 | 809 | 13 | 12 | 0 | 0 | SQL IO MVCC BT PUB | baseline deficit dominates; contention path not yet decisive |
| hot_page_contention | 4 | 5419 | 919 | 1322 | 17 | 24 | 38 | 0 | IO MVCC BT RETRY PUB | first clear sign MVCC can help under real contention |
| hot_page_contention | 8 | 1534 | 797 | 1436 | 52 | 94 | 79 | 9 | MVCC RETRY PUB | near-parity case; strongest evidence current thesis can work |
| mixed_read_write | 1 | 3378 | 108 | 98 | 3 | 3 | 0 | 0 | SQL READ IO MVCC BT PUB | whole-engine baseline overhead is severe |
| mixed_read_write | 4 | 2746 | 269 | 271 | 10 | 10 | 13 | 0 | SQL READ IO MVCC BT PUB | both modes still baseline-bound |
| mixed_read_write | 8 | 759 | 373 | 424 | 49 | 56 | 32 | 0 | READ IO MVCC BT PUB | some MVCC help, but broad engine cost still dominates |

#### Fixture: `frankentui`

| Workload | C | SQLite ops/s | SW ops/s | MVCC ops/s | SW% | MVCC% | SW retries | MVCC retries | Subsystems stressed | High-level signature |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|
| commutative_inserts_disjoint_keys | 1 | 6307 | 842 | 801 | 13 | 13 | 0 | 0 | SQL IO MVCC BT ALLOC PUB | same large baseline gap as `frankensqlite` fixture |
| commutative_inserts_disjoint_keys | 4 | 5335 | 603 | 967 | 11 | 18 | 42 | 26 | SQL IO MVCC BT ALLOC RETRY PUB | moderate MVCC lift, still nowhere close to SQLite |
| commutative_inserts_disjoint_keys | 8 | 1027 | 555 | 10 | 54 | 1 | 95 | 185 | IO MVCC BT ALLOC RETRY PUB | worst observed collapse; retry storm overwhelms everything |
| hot_page_contention | 1 | 5981 | 784 | 787 | 13 | 13 | 0 | 0 | SQL IO MVCC BT PUB | baseline deficit again dominant |
| hot_page_contention | 4 | 5531 | 964 | 1310 | 17 | 24 | 20 | 0 | IO MVCC BT RETRY PUB | MVCC improvement becomes visible |
| hot_page_contention | 8 | 1029 | 666 | 1446 | 65 | 141 | 74 | 10 | MVCC RETRY PUB | clearest outright MVCC win over SQLite in whole matrix |
| mixed_read_write | 1 | 3394 | 97 | 104 | 3 | 3 | 0 | 0 | SQL READ IO MVCC BT PUB | whole-engine baseline path still unacceptable |
| mixed_read_write | 4 | 2757 | 258 | 281 | 9 | 10 | 11 | 0 | SQL READ IO MVCC BT PUB | mostly baseline cost, not retry collapse |
| mixed_read_write | 8 | 1494 | 249 | 440 | 17 | 29 | 42 | 0 | READ IO MVCC BT PUB | MVCC helps some, but still heavily behind SQLite |

#### Fixture: `frankensearch`

| Workload | C | SQLite ops/s | SW ops/s | MVCC ops/s | SW% | MVCC% | SW retries | MVCC retries | Subsystems stressed | High-level signature |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|
| commutative_inserts_disjoint_keys | 1 | 6373 | 869 | 735 | 14 | 12 | 0 | 0 | SQL IO MVCC BT ALLOC PUB | large baseline gap again appears immediately |
| commutative_inserts_disjoint_keys | 4 | 5308 | 601 | 978 | 11 | 18 | 32 | 20 | SQL IO MVCC BT ALLOC RETRY PUB | same pattern as other fixtures at medium concurrency |
| commutative_inserts_disjoint_keys | 8 | 1514 | 680 | 13 | 45 | 1 | 144 | 183 | IO MVCC BT ALLOC RETRY PUB | near-total MVCC collapse with extreme retry count |
| hot_page_contention | 1 | 6786 | 794 | 936 | 12 | 14 | 0 | 0 | SQL IO MVCC BT PUB | baseline cost dominates, but MVCC slightly ahead of SW |
| hot_page_contention | 4 | 10017 | 927 | 1347 | 9 | 13 | 32 | 0 | IO MVCC BT RETRY PUB | SQLite is exceptionally strong here; MVCC still far behind |
| hot_page_contention | 8 | 1041 | 944 | 1160 | 91 | 111 | 92 | 10 | MVCC RETRY PUB | second outright MVCC win over SQLite in whole matrix |
| mixed_read_write | 1 | 439 | 107 | 58 | 24 | 13 | 0 | 0 | SQL READ IO MVCC BT PUB | even when SQLite is slow, FrankenSQLite is still much slower |
| mixed_read_write | 4 | 4694 | 313 | 287 | 7 | 6 | 8 | 0 | SQL READ IO MVCC BT PUB | baseline engine overhead dominates dramatically |
| mixed_read_write | 8 | 746 | 290 | 458 | 39 | 61 | 35 | 0 | READ IO MVCC BT PUB | better than SW, still clearly behind SQLite |

### Per-cell subsystem interpretation rules

To keep the matrix exhaustive without writing 27 long prose essays, each row above should be read with the following subsystem logic in mind.

For every `commutative_inserts_disjoint_keys` cell:

- `SQL` matters mostly at `c1`, because any fixed compile/setup cost is exposed directly.
- `BT` matters because inserts can force leaf growth, sibling creation, parent divider changes, and right-edge effects.
- `ALLOC` matters because "disjoint keys" can still converge on shared allocation metadata.
- `MVCC` matters because each page write pays ownership and validation cost.
- `RETRY` matters strongly at `c4` and especially `c8`, where false disjointness becomes visible.
- `PUB` matters because even independent page work must still be committed.

For every `hot_page_contention` cell:

- the workload is intentionally designed so `MVCC`, `RETRY`, and `PUB` are the center of gravity
- `BT` still matters because hot updates usually land on a narrow set of pages
- `c1` mostly shows baseline tax
- `c4` shows whether contention handling begins to pay off
- `c8` shows whether the concurrent design can outperform SQLite when hot overlap is unavoidable

For every `mixed_read_write` cell:

- `SQL`, `READ`, and `IO` matter much more than in the pure-write families
- `MVCC` still matters on the write half, but the row is also measuring general engine maturity
- bad results here are a broad indictment of the whole stack, not just concurrent write coordination
- if retries stay low but throughput is still bad, the likely explanation is whole-engine baseline overhead rather than pure conflict collapse

### Cross-matrix high-level takeaways from the full table

The full matrix makes several broad points unavoidable:

- The engine is not merely failing on one or two pathological rows. It loses badly across almost the whole table.
- The single-writer lane is too slow almost everywhere, which proves there is a large baseline efficiency deficit independent of MVCC.
- The concurrent-writer machinery does have real value in a narrow slice of the matrix, mainly `hot_page_contention` at `c8`.
- The flagship `commutative_inserts_disjoint_keys` family is the clearest evidence that SQL-level independence is not becoming physical-page independence in the current implementation.
- Retry counts become extreme exactly where MVCC collapses hardest, which is strong evidence that false shared surfaces plus expensive retry machinery are a central part of the disaster.

### Cross-matrix rollups by workload family and concurrency tier

Averaging across the three real fixtures gives a clearer shape of the performance frontier.

| Workload | C | Avg SQLite ops/s | Avg SW ops/s | Avg MVCC ops/s | Avg SW% of SQLite | Avg MVCC% of SQLite | What that says |
|---|---:|---:|---:|---:|---:|---:|---|
| commutative_inserts_disjoint_keys | 1 | 6291 | 809 | 781 | 13 | 12 | baseline insert path is already far too slow |
| commutative_inserts_disjoint_keys | 4 | 5345 | 583 | 989 | 11 | 19 | MVCC helps some, but still badly trails SQLite |
| commutative_inserts_disjoint_keys | 8 | 1834 | 587 | 32 | 32 | 2 | catastrophic MVCC collapse on the supposed showcase family |
| hot_page_contention | 1 | 6455 | 801 | 844 | 12 | 13 | baseline deficit still dominates |
| hot_page_contention | 4 | 6989 | 937 | 1326 | 13 | 19 | MVCC benefit starts to appear under real overlap |
| hot_page_contention | 8 | 1201 | 802 | 1347 | 67 | 112 | only family where MVCC broadly delivers on the thesis |
| mixed_read_write | 1 | 2404 | 104 | 87 | 4 | 4 | broad whole-engine baseline problem |
| mixed_read_write | 4 | 3399 | 280 | 280 | 8 | 8 | both modes still baseline-bound |
| mixed_read_write | 8 | 1000 | 304 | 441 | 30 | 44 | some MVCC help, but still clearly below SQLite |

This rollup is useful because it isolates the broad patterns:

- the engine is baseline-bad at `c1` across all families
- `hot_page_contention` is the only family where MVCC clearly scales toward or past SQLite
- `commutative_inserts_disjoint_keys` becomes the single clearest failure of the current architecture at `c8`
- `mixed_read_write` remains a broad system-performance problem, not just a concurrency problem

### Fixture-level characterization

The three fixtures are not interchangeable. They appear to stress different physical-layout properties.

#### `frankensqlite`

- useful as the most "home project" fixture
- shows the canonical disjoint-write collapse and near-parity hot-page case
- likely reflects a representative but not maximally pathological page-layout shape

#### `frankentui`

- produces the harshest collapse on `commutative_inserts_disjoint_keys c8`
- also produces the strongest MVCC win on `hot_page_contention c8`
- strongly suggests a layout where the difference between true hot-page wins and false-disjoint losses is extremely pronounced

#### `frankensearch`

- has some unusual mixed-workload shapes, including a very slow SQLite `mixed_read_write c1` baseline that FrankenSQLite still fails to capitalize on
- shows both strong hot-page behavior and extreme disjoint-write collapse
- suggests that row width, index mix, or structural shape may be especially important here

The key lesson is that fixes should not be judged on a single fixture. A change that helps one layout but worsens another can still be net harmful.

### Most informative focused row: `commutative_inserts_disjoint_keys / frankensqlite / c8`

This is the single most revealing row because it is supposed to be the project showcase. Instead it has been unstable and often catastrophic.

Important artifact snapshots:

- Deep-profile disaster:
  - [disjoint_c8_fsqlite_mvcc.stdout.json](/data/projects/frankensqlite/artifacts/perf/20260313_deep_profile/disjoint_c8_fsqlite_mvcc.stdout.json)
  - [disjoint_c8_sqlite3.stdout.json](/data/projects/frankensqlite/artifacts/perf/20260313_deep_profile/disjoint_c8_sqlite3.stdout.json)
  - SQLite: about `1026.79 ops/s`
  - FrankenSQLite MVCC: about `22.28 ops/s`

- Better current-tree run before the latest failed preclaim work:
  - [disjoint_c8_release_perf_both.jsonl](/data/projects/frankensqlite/artifacts/perf/20260314_direct_handle_owned_fastpath_pass3/disjoint_c8_release_perf_both.jsonl)
  - SQLite: about `1485.28 ops/s`
  - FrankenSQLite MVCC: about `684.07 ops/s`

- Another relatively decent run on a nearby tree:
  - [disjoint_c8_release_perf_both.jsonl](/data/projects/frankensqlite/artifacts/perf/20260314_direct_handle_owned_fastpath_v2/disjoint_c8_release_perf_both.jsonl)
  - SQLite: about `1027.36 ops/s`
  - FrankenSQLite MVCC: about `859.15 ops/s`

- Broad structural preclaim experiment that failed badly:
  - [disjoint_c8_release_perf_both.jsonl](/data/projects/frankensqlite/artifacts/perf/20260314_structural_preclaim/disjoint_c8_release_perf_both.jsonl)
  - SQLite: about `2794.48 ops/s`
  - FrankenSQLite MVCC: about `133.91 ops/s`

- Parent-only structural preclaim experiment that was even worse:
  - [disjoint_c8_release_perf_both.jsonl](/data/projects/frankensqlite/artifacts/perf/20260314_parent_preclaim/disjoint_c8_release_perf_both.jsonl)
  - SQLite completed at about `2935.38 ops/s`
  - FrankenSQLite MVCC never completed before manual termination

### Lower-concurrency data is also bad

From [sqlite_plus_mvcc_nowarm_repeat1.jsonl](/data/projects/frankensqlite/artifacts/perf/20260314_commit_planning_fix/sqlite_plus_mvcc_nowarm_repeat1.jsonl):

- `c1` on `commutative_inserts_disjoint_keys / frankensqlite`
  - SQLite: `6893.01 ops/s`
  - MVCC: `840.66 ops/s`

- `c4` on `commutative_inserts_disjoint_keys / frankensqlite`
  - SQLite: `5346.89 ops/s`
  - MVCC: `1099.75 ops/s`

This matters because it shows the problem is not only high-concurrency lock contention. FrankenSQLite also has major baseline overhead at lower concurrency.

### Matrix-wide diagnosis by workload family

The complete whole-suite matrix is important because it shows the gap is not one single bug. Different workload families fail in different ways.

#### 1. `commutative_inserts_disjoint_keys`

This should be the flagship workload for the project, because if writers really are disjoint at the page level, MVCC should shine here.

What the complete matrix shows:

- On `frankensqlite`
  - `c1`: SQLite `6193 ops/s`, MVCC `807 ops/s`, single-writer `717 ops/s`
  - `c4`: SQLite `5391`, MVCC `1021`, single-writer `545`
  - `c8`: SQLite `2960`, MVCC `74`, single-writer `527`

- On `frankentui`
  - `c1`: SQLite `6307`, MVCC `801`, single-writer `842`
  - `c4`: SQLite `5335`, MVCC `967`, single-writer `603`
  - `c8`: SQLite `1027`, MVCC `10`, single-writer `555`

- On `frankensearch`
  - `c1`: SQLite `6373`, MVCC `735`, single-writer `869`
  - `c4`: SQLite `5308`, MVCC `978`, single-writer `601`
  - `c8`: SQLite `1514`, MVCC `13`, single-writer `680`

What that implies:

- At `c1`, MVCC is already only about `0.11x` to `0.13x` of SQLite. That is a baseline engine cost problem, not a contention problem.
- At `c4`, MVCC improves relative to its own `c1` numbers, but it is still only about `0.18x` to `0.19x` of SQLite. That means there is still major overhead before the worst contention effects even arrive.
- At `c8`, MVCC collapses catastrophically on all three fixtures in the complete baseline snapshot, much worse than single-writer. That strongly suggests an MVCC-specific conflict amplification problem on top of the baseline gap.
- Single-writer is also far behind SQLite, but it does not collapse nearly as badly as MVCC on `c8`. That isolates a large part of the catastrophic `c8` failure to concurrent write coordination, not just generic engine slowness.

This is the clearest evidence that "disjoint" SQL-level inserts are still not disjoint in the real storage structures.

#### 2. `hot_page_contention`

This workload is the one place where the project's core idea actually shows through.

What the complete matrix shows:

- On `frankensqlite`
  - `c1`: SQLite `6598`, MVCC `809`, single-writer `826`
  - `c4`: SQLite `5419`, MVCC `1322`, single-writer `919`
  - `c8`: SQLite `1534`, MVCC `1436`, single-writer `797`

- On `frankentui`
  - `c1`: SQLite `5981`, MVCC `787`, single-writer `784`
  - `c4`: SQLite `5531`, MVCC `1310`, single-writer `964`
  - `c8`: SQLite `1029`, MVCC `1446`, single-writer `666`

- On `frankensearch`
  - `c1`: SQLite `6786`, MVCC `936`, single-writer `794`
  - `c4`: SQLite `10017`, MVCC `1347`, single-writer `927`
  - `c8`: SQLite `1041`, MVCC `1160`, single-writer `944`

What that implies:

- At `c1`, MVCC and single-writer are both still only about `0.12x` to `0.14x` of SQLite. Again, the baseline cost is poor before concurrency benefits matter.
- At `c4`, MVCC begins to outperform FrankenSQLite single-writer meaningfully, but it is still far behind SQLite.
- At `c8`, MVCC is near parity or better on this family: about `0.94x` on `frankensqlite`, `1.40x` on `frankentui`, and `1.11x` on `frankensearch`.

This is the strongest evidence that the core concurrent-writer thesis is not fundamentally impossible. When the conflict pattern matches the engine's strengths, MVCC can beat its own single-writer mode and sometimes beat SQLite. But the win is much smaller and much rarer than the project goal requires.

#### 3. `mixed_read_write`

This family is important because it tests not just writer conflict handling, but general end-to-end engine cost.

What the complete matrix shows:

- On `frankensqlite`
  - `c1`: SQLite `3378`, MVCC `98`, single-writer `108`
  - `c4`: SQLite `2746`, MVCC `271`, single-writer `269`
  - `c8`: SQLite `759`, MVCC `424`, single-writer `373`

- On `frankentui`
  - `c1`: SQLite `3394`, MVCC `104`, single-writer `97`
  - `c4`: SQLite `2757`, MVCC `281`, single-writer `258`
  - `c8`: SQLite `1494`, MVCC `440`, single-writer `249`

- On `frankensearch`
  - `c1`: SQLite `439`, MVCC `58`, single-writer `107`
  - `c4`: SQLite `4694`, MVCC `287`, single-writer `313`
  - `c8`: SQLite `746`, MVCC `458`, single-writer `290`

What that implies:

- The mixed workload is not just a concurrency problem. At `c1`, both FrankenSQLite modes are often only around `0.03x` to `0.13x` of SQLite.
- MVCC does help somewhat at higher concurrency versus FrankenSQLite single-writer, especially at `c8`, but it still remains well below SQLite.
- The `frankensearch c1` row is especially useful because SQLite itself is relatively slow there at `439 ops/s`, yet MVCC is still only `58 ops/s`. So FrankenSQLite loses badly even on rows where SQLite is not at peak throughput.

This family strongly suggests that broad end-to-end engine overhead remains too high in the parser/VDBE/storage path, independent of the specific concurrent write conflict story.

### Matrix-wide diagnosis by engine mode

The single-writer lane is extremely informative because it tells us what part of the gap is not caused by MVCC.

Key facts from the complete matrix:

- Single-writer beats SQLite in `0/27` rows.
- Single-writer is often in the same rough band as MVCC at low concurrency.
- On `hot_page_contention c8`, MVCC materially beats single-writer.
- On `commutative_inserts_disjoint_keys c8`, MVCC is often much worse than single-writer.

What that implies:

- There is a large baseline engine gap that exists even without concurrent MVCC wins or losses.
- MVCC is not the whole problem. Even if MVCC overhead vanished, the single-writer lane still shows a large deficit versus SQLite.
- But MVCC also introduces a second, separate problem on the disjoint-insert family: it can amplify conflicts so badly that it performs much worse than FrankenSQLite's own single-writer mode.

The cleanest way to say it is:

- `single-writer vs SQLite` measures the baseline engine gap.
- `MVCC vs single-writer` measures the incremental benefit or harm of concurrent-writer machinery.
- Right now the baseline gap is already large, and the MVCC machinery only helps in a narrow slice of cases.

### What the profiling and counters suggest, and what they do not

There are several plausible explanations for the performance gap. The profiling evidence narrows them down.

#### 1. Wall-clock time is being burned in the write path and retry machinery

On the catastrophic `disjoint c8` row, the strongest evidence remains:

- [disjoint_c8_fsqlite_mvcc.perfreport.txt](/data/projects/frankensqlite/artifacts/perf/20260313_deep_profile/disjoint_c8_fsqlite_mvcc.perfreport.txt)
- [disjoint_c8_fsqlite_mvcc.perfstat.csv](/data/projects/frankensqlite/artifacts/perf/20260313_deep_profile/disjoint_c8_fsqlite_mvcc.perfstat.csv)

Important facts:

- about `95%` of samples were in the write path around [engine.rs:880](/data/projects/frankensqlite/crates/fsqlite-vdbe/src/engine.rs:880)
- about `47.1M` instructions per op for MVCC versus about `0.79M` for SQLite
- IPC about `0.046` for MVCC versus about `0.90` for SQLite

That is consistent with:

- heavy lock/wait/retry churn
- many repeated checks and restaging operations
- lots of cycles spent stalled or spinning through coordination logic rather than retiring useful work

#### 2. This is not primarily a cache-locality story

The cache data on the catastrophic row does not support "poor cache locality is the main reason" as the leading explanation.

On the same `disjoint c8` deep-profile run:

- MVCC cache-miss rate was about `19.5%`
- SQLite cache-miss rate was about `21.5%`

That does not mean cache behavior is irrelevant, but it strongly argues that cache locality is not the first-order explanation for the gigantic throughput gap.

#### 3. This is not mainly a "fails to use multiple cores" story

On the catastrophic `disjoint c8` run:

- SQLite used about `0.19 CPUs`
- MVCC used about `5.7 CPUs`

So MVCC is not sitting single-threaded while SQLite races ahead. It is using multiple cores, but those cores are not doing productive work efficiently.

The correct interpretation is:

- SQLite is fast with little parallel CPU because its uncontended single-writer path is highly efficient.
- FrankenSQLite MVCC spreads work across cores, but the work is low-IPC, coordination-heavy, and retry-heavy.

This is wasted parallelism, not lack of parallelism.

#### 4. Parser/AST/row decode overhead is real, but it is not the whole explanation

The internal hot-profile artifacts for the same workload are:

- [summary.md](/data/projects/frankensqlite/artifacts/perf/20260313_profile_drilldown/disjoint_c8_frankensqlite_mvcc/summary.md)
- [profile.json](/data/projects/frankensqlite/artifacts/perf/20260314_hot_profile_disjoint_c8_current/profile.json)

Those artifacts show:

- parser + compile time in the hundreds of microseconds
- record decode time in the low single-digit milliseconds
- row materialization time in the sub-millisecond to ~1 millisecond range

Compared to a wall time around `10.4s`, those subsystems are not enough to explain the catastrophic disjoint-write row.

However, they still matter for the broader matrix:

- single-writer is also far behind SQLite
- mixed read/write remains poor even where retries are not the full story
- the mixed deep profile also shows nontrivial cost in `write_page`, `IoUringFile::read`, `malloc`, `memmove`, and VDBE setup

So parser and row materialization are not the dominant cause of the worst concurrent-write disaster, but they are still part of the broad baseline gap.

#### 5. I/O and memory-copy overhead are also part of the mixed-workload gap

The mixed `c8` perf artifacts are:

- [mixed_c8_fsqlite_mvcc.perfstat.csv](/data/projects/frankensqlite/artifacts/perf/20260313_deep_profile/mixed_c8_fsqlite_mvcc.perfstat.csv)
- [mixed_c8_fsqlite_mvcc.perfreport.txt](/data/projects/frankensqlite/artifacts/perf/20260313_deep_profile/mixed_c8_fsqlite_mvcc.perfreport.txt)

Important facts:

- MVCC on this row had IPC about `0.79`, much better than the catastrophic disjoint row
- the top sampled function was still `SharedTxnPageIo::write_page`
- `IoUringFile::read`, `malloc`, `_int_malloc`, `memmove`, and cursor/page-pointer work all showed up in the hot report

That suggests:

- on mixed workloads, the engine is not primarily failing because of wild lock convoy collapse
- instead it is paying a broad cost in page I/O, page copying, allocation churn, and engine setup/teardown

This lines up with the matrix-wide observation that single-writer is also far behind SQLite.

### Likely sources of the overall performance gap, ranked by importance

Based on the full matrix and profiling evidence, the likely gap sources are:

#### Tier 1: highest confidence

- **Shared structural-page conflict amplification during inserts**
  - strongest evidence: `commutative_inserts_disjoint_keys c8` collapses in MVCC but not nearly as hard in single-writer
  - likely mechanism: parent/internal/divider/right-edge pages become convoy points

- **Excessively expensive MVCC write-path coordination**
  - strongest evidence: [engine.rs:880](/data/projects/frankensqlite/crates/fsqlite-vdbe/src/engine.rs:880) dominates profiles, with terrible IPC and huge instruction counts
  - likely mechanism: repeated lock checks, commit-index checks, staging, restore logic, and retry loops

- **Large baseline engine overhead even without MVCC wins/losses**
  - strongest evidence: single-writer loses badly to SQLite on almost every row, including `c1`
  - likely mechanism: extra copies, staging, planner/VDBE overhead, page handling, and engine setup cost

#### Tier 2: high but not primary

- **Allocator/page-1 metadata false sharing**
  - strongest evidence: earlier debugging showed allocator operations still drag page 1 into the conflict surface
  - likely effect: SQL-level disjoint work is not disjoint in page-space terms

- **Page copying / heap allocation / memmove churn**
  - strongest evidence: mixed-workload perf report shows `malloc`, `_int_malloc`, `memmove`, `PageData::into_vec`, and VFS reads
  - likely effect: hurts both MVCC and single-writer, especially on mixed read/write

- **Residual centralized coordination**
  - strongest evidence: commit-index, lock-table, and other central structures still exist in hot paths
  - likely effect: adds more shared coordination cost than SQLite's tuned uncontended write path

#### Tier 3: plausible but not leading

- **Poor cache locality**
  - evidence does not support this as the main cause of the large gap

- **Failure to exploit all cores**
  - evidence actively argues against this explanation; MVCC uses cores, but inefficiently

### Additional broad hypotheses worth keeping in mind

The following causes are not yet top-ranked by evidence, but they are still technically plausible contributors to the extreme gap.

#### 1. Compatibility-path tax versus SQLite's fully mature hot path

FrankenSQLite is trying to compete with SQLite while still honoring SQLite-compatible storage behavior and while some fallback or compatibility-oriented execution surfaces still exist in the current runtime.

Why this matters:

- SQLite's hot path is decades-old and ruthlessly tuned
- FrankenSQLite may be paying extra abstraction, layering, or fallback costs that a mature monolithic C path does not pay
- some workloads may traverse less-mature engine surfaces more often than expected

This is unlikely to explain the worst concurrent-insert collapse by itself, but it could explain part of the broad single-writer and low-concurrency deficit.

#### 2. Page-level MVCC may be fighting the grain of SQLite-style structural pages

Page-level MVCC is attractive because it preserves the file-format and B-tree worldview. But SQLite's page layout was designed for a serialized writer model, not for many concurrent writers mutating nearby structure.

Why this matters:

- parent and right-edge updates can become disproportionately important
- a page-level concurrency scheme may inherit structural hot spots that row-level systems avoid differently
- some conflict surfaces may be "designed in" unless the B-tree growth policy is changed substantially

This is one of the core strategic risks of the current architecture.

#### 3. Serializable validation overhead may be too expensive in the current implementation

The concurrent path is not just taking page locks. It is also trying to preserve strong semantics.

Why this matters:

- read and write witness tracking can add per-operation cost
- snapshot validation and commit-time checks can add fixed overhead
- if those checks are implemented conservatively or redundantly, their cost compounds across retries

Current evidence says this is real, though not yet isolated as the dominant single sub-cause.

#### 4. Overflow pages and wide-row behavior may amplify physical conflict surfaces

The real databases in the corpus have different row widths, index mixes, and payload shapes.

Why this matters:

- wider rows can alter leaf occupancy
- overflow chains can create more page touches per logical row operation
- fewer rows per page can increase split frequency and allocator pressure

This may help explain why fixture behavior diverges and why some databases exhibit much harsher collapse geometry than others.

#### 5. Too much per-operation state repair in the failure path

Several fixes have been necessary around rollback, stale synthetic state, page-one cleanup, and savepoint behavior.

Why this matters:

- it suggests the failure path is complicated
- any complexity in the failure path becomes a throughput problem when retries are frequent
- restoring correctness after each failed attempt may itself become a large fraction of total time

This is one reason the gap can explode under contention rather than merely degrade smoothly.

#### 6. Layering and ownership churn across many crates may be creating hidden constant factors

The workspace decomposition is architecturally clean, but it may have nontrivial constant costs in the hot path.

Why this matters:

- page data may cross multiple ownership and conversion boundaries
- generic abstractions can inhibit the kind of highly specialized fast path SQLite has
- even safe, correct abstractions can add enough per-operation work to be visible against SQLite

This hypothesis mainly targets the baseline gap rather than the catastrophic concurrent collapse.

### Subsystem-by-subsystem map of where the gap could be coming from

Another useful way to look at the problem is by subsystem rather than by benchmark family.

#### 1. SQL frontend and compilation

Relevant layers:

- parser
- name resolution / planning
- VDBE/codegen
- statement setup and teardown

Why this could matter:

- every statement pays some fixed cost before any useful storage work happens
- if those fixed costs are materially above SQLite's, `c1` and mixed-workload rows will look bad even without serious contention

What the evidence says:

- this is probably a real contributor to the broad baseline gap
- it is probably not the primary explanation for the catastrophic `disjoint c8` collapse
- it matters more for mixed and low-concurrency rows than for the worst retry storm

#### 2. Pager, VFS, and page-buffer handling

Relevant layers:

- page fetch
- page normalization
- read/write buffer ownership changes
- VFS read/write path

Why this could matter:

- every read or write can pay for buffer reshaping, cloning, zero-fill, or copy work
- even if concurrency were perfect, this would still hurt the single-writer lane

What the evidence says:

- mixed workload profiles show `IoUringFile::read`, `malloc`, `_int_malloc`, and `memmove`
- this is a strong candidate for part of the baseline gap
- this does not by itself explain why MVCC can collapse much worse than FrankenSQLite single-writer

#### 3. MVCC write staging and page ownership bookkeeping

Relevant layers:

- `SharedTxnPageIo::write_page_internal`
- page-lock ownership
- snapshot validation
- staged page state
- restore/rollback bookkeeping

Why this could matter:

- every page write can require more checks and more state transitions than SQLite performs
- any redundant bookkeeping here gets multiplied by every row and every retry

What the evidence says:

- this is one of the strongest current suspects
- the dominant hot path sits here
- it clearly contributes both to baseline cost and to retry amplification

#### 4. B-tree structural maintenance

Relevant layers:

- leaf splits
- parent divider updates
- right-edge growth
- non-root rebalance paths

Why this could matter:

- logical SQL independence does not imply physical page independence
- if many inserts funnel through the same parent or internal pages, MVCC degenerates into a convoy

What the evidence says:

- this is the strongest architectural explanation for the `disjoint` workload collapse
- recent experiments show that "claim the shared structure earlier" makes things worse, not better
- the engine likely needs to avoid shared structural mutation more often, not just coordinate it differently

#### 5. Allocator, freelist, and page-one metadata

Relevant layers:

- page allocation/free
- page-one conflict surface
- pending commit surface around allocator-visible pages

Why this could matter:

- independent inserts can still collide if they all touch the same allocator metadata
- this creates false sharing unrelated to user-visible row independence

What the evidence says:

- this is a real source of false conflict
- it is probably not the only reason for the worst rows, but it likely compounds the structural B-tree problem

#### 6. Wait path, wake path, and retry policy

Relevant layers:

- page-lock conflict handling
- park/wake or retry loops
- benchmark-level busy retry backoff

Why this could matter:

- once contention begins, poor retry behavior can turn a modest conflict rate into wall-clock disaster
- the engine can spend time not only losing, but losing expensively

What the evidence says:

- this is clearly part of the catastrophic rows
- the live profiles and thread states show workers using CPU or sleeping while making little useful progress
- it is not enough to reduce retries; the work done per retry also has to shrink dramatically

#### 7. Commit/publication and central coordination surfaces

Relevant layers:

- commit index
- shared lock tables
- publish sequencing
- pending commit page construction

Why this could matter:

- even if page-level concurrency is good, centralized publication can reintroduce choke points
- any central structure in a hot path is dangerous if it is touched frequently enough

What the evidence says:

- this remains a meaningful concern
- current evidence suggests it is not the first-order reason the flagship `disjoint` row collapses
- it likely becomes more important once the bigger structural conflicts are reduced

### Why the regressions can be extreme rather than modest

The current failures are not just additive. They are multiplicative.

At a high level, the bad rows look like this:

1. FrankenSQLite starts with a worse baseline cost than SQLite on each operation.
2. The workload begins to create physical-page conflicts that the SQL shape does not make obvious.
3. Each conflict triggers retries, waits, or abort/restart work.
4. Each retry re-enters a path that is already more expensive than SQLite's base path.
5. Structural pages and allocator metadata cause many writers to collide again.
6. Wall-clock time explodes, IPC collapses, and throughput falls off a cliff.

That is why the results can go from "still too slow" at `c1` to "utter collapse" at `c8`.

This is also why some ideas can backfire so badly. A change that slightly increases the duration of shared structural ownership does not just add a small constant cost. It can:

- increase the number of waiting writers
- increase the probability of retry cascades
- increase the amount of wasted work each losing writer performs
- turn a partially concurrent workload into an effectively serialized convoy

That is what made the structural preclaim experiments so damaging. They did not merely fail to help. They made the multiplicative part of the bad feedback loop worse.

### Why fixture-specific variance still matters

The three real fixtures do not behave identically:

- `frankensqlite`
- `frankentui`
- `frankensearch`

That is useful, not noise. It suggests the conflict surface depends materially on real B-tree shape and database contents:

- fanout and depth
- leaf occupancy
- hot right-edge behavior
- free-page distribution
- index/table mix
- row width and overflow behavior

So the problem is not just "the engine is slow in the abstract." It is also that some physical layouts create much worse collision geometry than others. Any future fix should therefore be judged not just by one fixture, but by whether it makes the whole corpus less sensitive to layout shape.

### What the current evidence still does not prove

Some hypotheses remain plausible but are not yet strongly established.

- We do not yet have proof that one specific allocator redesign will solve the disjoint-write collapse.
- We do not yet have proof that an optimistic micro-publish scheme for parent updates will outperform the current approach.
- We do not yet know exactly how much of the single-writer gap comes from frontend/VDBE work versus pager/VFS work versus page-buffer churn.
- We do not yet know whether the current mixed-workload gap is mostly a storage-path problem or a broader whole-engine efficiency problem.

That uncertainty is important. The report should not pretend we know more than we do. But we do know enough to say the following with high confidence:

- the performance disaster is real
- it is not caused by a single bug
- it is not mainly caused by cache misses
- it is not mainly caused by failure to use cores
- it is not solved by claiming shared structural pages earlier
- it does involve both a baseline efficiency deficit and a concurrency-conflict amplification deficit

### A more complete current interpretation

The broadest accurate summary is:

- FrankenSQLite currently has **two overlapping performance problems**.
- First, it has a **baseline engine-efficiency problem**. That is why even single-writer and low-concurrency rows are far behind SQLite.
- Second, it has a **concurrent structural-conflict problem**. That is why the disjoint-write showcase row can collapse much worse than single-writer.

The project is therefore not stuck on just one bug. It needs:

- one line of work to reduce baseline cost
- another line of work to stop "disjoint" concurrent inserts from converging on shared structural surfaces

The failed structural preclaim experiments are now useful precisely because they falsified one tempting but wrong idea: making shared structural ownership earlier and longer does not help. It worsens the convoy.

## Biggest Known Problems

### 1. The MVCC write hot path is still far too expensive

The central hot path is [engine.rs:880](/data/projects/frankensqlite/crates/fsqlite-vdbe/src/engine.rs:880), `SharedTxnPageIo::write_page_internal`.

This path currently does too much:

- page-1 conflict-surface logic
- concurrent handle locking
- commit-index stale-snapshot checking
- page-lock acquisition or wait/retry
- MVCC page staging
- pager write staging and restore logic
- synthetic pending-commit-surface cleanup

The best profiling evidence for this is:

- [disjoint_c8_fsqlite_mvcc.perfstat.csv](/data/projects/frankensqlite/artifacts/perf/20260313_deep_profile/disjoint_c8_fsqlite_mvcc.perfstat.csv)
- [disjoint_c8_fsqlite_mvcc.perfreport.txt](/data/projects/frankensqlite/artifacts/perf/20260313_deep_profile/disjoint_c8_fsqlite_mvcc.perfreport.txt)

The key facts from that profile were:

- about `95%` of sampled cycles were in the write path
- about `47.1M` instructions per op for MVCC versus about `0.79M` for SQLite
- IPC around `0.046`, which is terrible
- several cores were busy, but mostly doing stalled or retry-heavy work

So the problem is not "FrankenSQLite fails to use cores." It is using cores to do expensive, low-IPC, conflict-heavy work.

### 2. "Disjoint" inserts are not truly disjoint in engine terms

There are at least two reasons:

- allocator/page-1 metadata conflicts
- shared B-tree structural page conflicts

The page-1 issue shows up in the concurrent path around:

- [engine.rs:1409](/data/projects/frankensqlite/crates/fsqlite-vdbe/src/engine.rs:1409)
- [engine.rs:1458](/data/projects/frankensqlite/crates/fsqlite-vdbe/src/engine.rs:1458)

Earlier investigation showed that `allocate_page`, `free_page`, and some writes still drag page 1 into the conflict surface. That means transactions that look disjoint at the SQL level still collide on shared metadata.

### 3. B-tree structure changes converge many writers onto the same parent/internal pages

The critical structural code is:

- [balance.rs:389](/data/projects/frankensqlite/crates/fsqlite-btree/src/balance.rs:389), `balance_nonroot`
- [balance.rs:962](/data/projects/frankensqlite/crates/fsqlite-btree/src/balance.rs:962), `prepare_leaf_table_local_split`
- [balance.rs:1077](/data/projects/frankensqlite/crates/fsqlite-btree/src/balance.rs:1077), `balance_table_leaf_local_split`
- [balance.rs:1606](/data/projects/frankensqlite/crates/fsqlite-btree/src/balance.rs:1606), `apply_child_replacement`

The key problem is that many logically separate inserts still converge on:

- the same parent page
- the same divider update path
- the same right-edge/internal-page rewrite logic

This means the workload named `commutative_inserts_disjoint_keys` is not actually disjoint at the B-tree structure level.

### 4. Retry amplification is still severe

The retry/abort counts on the key row remain high even in improved runs:

- about `144` retries/aborts in the deep-profile disaster
- about `152` in one improved run
- about `162` in another improved run
- about `168` in the failed broad-preclaim run

This means even when throughput looks less catastrophic than `22 ops/s`, the engine is still spending too much time colliding, retrying, and restaging.

### 5. There is still baseline overhead even when contention is not the dominant problem

The `c1` and `c4` numbers show this clearly. At low concurrency, the engine is still far behind SQLite, which means there is fundamental per-operation cost outside of lock contention.

Likely contributors include:

- extra page staging and copies
- too much bookkeeping per page write
- more expensive commit planning / pending surface construction
- parser / VDBE / row materialization overhead in mixed workloads

### 6. Residual global or centralized coordination still exists

Some earlier pathologies were caused by globally central logic in or around:

- the concurrent registry
- the shared page lock table
- the commit index
- the compatibility publish path

Some of this has been improved, but the design still has central coordination surfaces that are much more expensive than SQLite's tuned single-writer fast path on uncontended work.

## What Has Already Been Tried

### 1. Benchmark fixture and corpus fixes

What was done:

- fixed the benchmark to use the canonical pinned working-copy fixture paths instead of naive defaults
- collected larger, richer sample DBs including Agent Mail stores and other corpora

What it accomplished:

- improved benchmark validity
- did not improve engine performance directly

### 2. Generalized profiling and hot-profile capture

What was done:

- expanded profiling coverage beyond only `mixed_read_write`
- profiled the actual `commutative_inserts_disjoint_keys / c8` hotspot

What it accomplished:

- this was useful and necessary
- it identified [engine.rs:880](/data/projects/frankensqlite/crates/fsqlite-vdbe/src/engine.rs:880) as the dominant hot path
- it established that the problem is heavy write-path work and retry churn, not merely idle cores

### 3. Row-cache and hot-path micro-optimizations in the VDBE/storage path

What was done:

- fixed or improved row-cache behavior in the storage cursor path
- reduced some redundant page/row decode work

What it accomplished:

- these changes were useful for baseline overhead
- they were not the first-order fix for the catastrophic `disjoint c8` row

### 4. Direct-handle / already-owned-page fast paths

What was done:

- moved away from rediscovering transaction state through more expensive lookup patterns
- improved fast paths when the same transaction already owns the page
- fixed savepoint rollback behavior so already-owned pages could be restaged correctly

What it accomplished:

- this was a real improvement
- it helped move `disjoint c8` from catastrophic values into the `684-859 ops/s` range on some trees
- it did **not** get us to SQLite parity

### 5. Page-one synthetic tracking cleanup

What was done:

- fixed synthetic page-one tracking cleanup and related restore logic

What it accomplished:

- it removed at least one source of false conflict and state corruption
- it did not solve the core concurrent insert bottleneck

### 6. Commit-planning regression fix

What was done:

- fixed a correctness regression in [connection.rs:8873](/data/projects/frankensqlite/crates/fsqlite-core/src/connection.rs:8873), [connection.rs:12952](/data/projects/frankensqlite/crates/fsqlite-core/src/connection.rs:12952), [connection.rs:13021](/data/projects/frankensqlite/crates/fsqlite-core/src/connection.rs:13021), [connection.rs:13248](/data/projects/frankensqlite/crates/fsqlite-core/src/connection.rs:13248), and [connection.rs:13262](/data/projects/frankensqlite/crates/fsqlite-core/src/connection.rs:13262)
- the bug was that commit planning tried to pull pending pages from `self.active_txn` after autocommit had already detached the transaction handle

What it accomplished:

- restored correct MVCC execution instead of crashing with internal errors
- got the benchmark matrix back to the old bad frontier
- improved correctness, not performance

### 7. Structural preclaim experiment: full structural page set

What was done:

- temporarily added a `preclaim_write_pages` hook through [cursor.rs](/data/projects/frankensqlite/crates/fsqlite-btree/src/cursor.rs)
- temporarily added concurrent preclaim/rollback logic in [engine.rs](/data/projects/frankensqlite/crates/fsqlite-vdbe/src/engine.rs)
- tried preclaiming the structural page set before split/rebalance writes

What it accomplished:

- correctness of the helper is fine
- the benchmark result was terrible
- on the key row it dropped MVCC to about `133.91 ops/s`
- the code has now been reverted, so this is historical evidence only

Why it failed:

- it widened the claimed surface and held shared pages earlier
- that created a convoy effect instead of removing contention

### 8. Structural preclaim experiment: parent-only

What was done:

- narrowed the preclaim to just the shared parent hotspot

What it accomplished:

- correctness again was fine
- performance was even worse in practice
- SQLite finished immediately, and MVCC did not finish before manual termination
- this experiment has also now been reverted

Why it failed:

- it still turned the parent into a long-lived serialized choke point
- it confirms that "claim shared structure earlier" is the wrong direction

### 9. No-op write elision and unchanged-page preservation

What was done:

- introduced or strengthened logic to avoid rewriting pages when the final page image is unchanged
- preserved original page images more carefully during structural operations and rollback paths

What it accomplished:

- this reduced some unnecessary page churn
- it improved correctness around rollback and reduced avoidable writes
- it was beneficial, but clearly not enough to overcome the dominant conflict and coordination costs

### 10. Local leaf-split fast path work

What was done:

- added and refined a more local `LeafTable` split path intended to avoid immediately falling into broader sibling rebalance work
- later fixed a concrete bug where the local split path was too eager when the parent lacked room

What it accomplished:

- this was a reasonable direction because it tries to reduce shared structural work
- however, the implementation proved delicate, and at least one version worsened behavior before the parent-capacity gate was fixed
- the broader idea still seems directionally better than preclaim, but it is not yet a decisive win

### 11. Fresh-eyes audit and correctness cleanup on touched paths

What was done:

- reread recent performance-related edits looking for obvious correctness bugs and edge cases
- fixed issues such as wrong fast-path assumptions, oversized page-buffer handling, and page-one cleanup/reconciliation bugs

What it accomplished:

- prevented some benchmark and runtime results from being polluted by correctness regressions
- improved confidence that the current bad performance is mostly "real slowness," not just hidden breakage
- did not materially close the core throughput gap on its own

### 12. Revert of the structural preclaim experiment

What was done:

- manually reverted the explicit structural preclaim hook and its call sites without reverting unrelated or earlier non-preclaim improvements

What it accomplished:

- returned the codebase to a cleaner preclaim-free state
- removed a known bad idea from the live tree
- preserved the failed benchmark artifacts as historical evidence

### 13. Fresh release-perf builds and repeated focused reruns

What was done:

- rebuilt fresh binaries
- reran the benchmark matrix and focused rows multiple times after major code changes
- compared against prior artifacts to distinguish correctness regressions from genuine performance movement

What it accomplished:

- established that some fixes only restored the old bad frontier rather than improving it
- identified the difference between "fixed a regression" and "actually moved the performance frontier"
- provided the historical record needed to see which ideas helped a little, helped meaningfully, or actively made things worse

## What Has Worked Versus What Has Not

### What has worked

- fixing benchmark fixture resolution and corpus quality
- fixing the commit-planning regression
- profiling the actual bad row instead of generic workloads
- direct-handle / owned-page fast paths
- some row-cache and redundant bookkeeping reductions
- no-op page-write elision and some rollback hygiene improvements
- reverting the known-bad structural preclaim experiment
- repeatedly rerunning the same focused rows after each major change so false progress is easier to catch

### What has not worked

- broad structural preclaim
- parent-only structural preclaim
- the general intuition that earlier deterministic page claiming would remove the bottleneck
- relying on more measurement without turning the findings into code changes
- keeping the failed structural-preclaim code landed
- assuming that a correctness regression fix is also a performance fix
- assuming that a narrower structural lock scope automatically means a shorter effective convoy

## Broader Technical Failure Modes To Pressure-Test

Even with the evidence already in hand, it is useful to keep a broader failure-mode catalog in mind so future experiments stay grounded.

### 1. Work inflation per logical operation

One SQL insert/update may simply translate into far more engine work than in SQLite:

- more pages touched
- more state tracked
- more copies created
- more checks performed

This is the most basic way to lose to SQLite.

### 2. Hidden serialization despite explicit concurrent design

The engine can claim to be concurrent at the API level while still serializing in practice on:

- parent pages
- allocator metadata
- shared registries
- commit publication

This is how a design can be "concurrent on paper" and still disappoint badly in real workloads.

### 3. Feedback loops between conflict and repair work

The engine may not just lose once when a conflict happens. It may:

- detect a conflict
- repair or restore state
- re-enter the expensive path
- collide again

That turns contention into a positive feedback loop.

### 4. Mature SQLite specialization versus generality tax

SQLite has an enormous advantage in maturity:

- fewer layers
- fewer ownership transitions
- fewer defensive restore paths
- more locally specialized code in exactly the hot operations it cares about

Any generalized or cleaner design in FrankenSQLite has to overcome that constant-factor disadvantage before its concurrency wins can matter.

### 5. Physical-layout sensitivity

The engine may perform acceptably on some page layouts and catastrophically on others. That means fixes must not merely improve an average case; they need to reduce sensitivity to:

- tree depth
- page fullness
- right-edge growth
- allocator state
- wide rows and overflow usage

### 6. Correctness-preserving but throughput-poisoning conservatism

A system can be perfectly correct and still disastrously slow if it preserves correctness through:

- too many barriers
- too much defensive copying
- too many restore paths
- too conservative a conflict surface

One of the core challenges here is to stop paying correctness costs in places where they are not actually needed, without weakening the semantics.

### 7. Work-surface mismatch between logical operations and physical operations

This is one of the most dangerous classes of failure in a page-oriented MVCC engine.

At the SQL level, a benchmark might say:

- "these inserts touch different keys"
- "these writers should commute"
- "these updates are independent"

But the storage engine may translate that into:

- same parent page
- same allocator metadata page
- same right-edge growth path
- same commit/publication hotspot

When that happens, the logical workload description becomes misleading. The benchmark is no longer measuring the idealized operation shape. It is measuring the engine's translation of that shape into page-space. This is likely one of the main reasons the "disjoint inserts" family is such a disaster.

### 8. Local optimizations that worsen global queueing behavior

Some performance ideas can be locally rational and globally harmful.

Examples:

- claiming a shared page earlier to avoid discovering contention later
- holding more structural context to reduce retries inside one transaction
- widening a fast path without carefully modeling what it does to shared ownership duration

The structural preclaim experiments are the clearest example so far. They looked like they would reduce wasted mid-operation work, but globally they worsened queueing and convoy duration.

### 9. Mature-engine asymmetry

SQLite is not merely "another implementation." It is an implementation that has had decades of specialization pressure.

That means FrankenSQLite is not only competing against an algorithmic baseline. It is competing against:

- path length that has been shaved relentlessly
- branch structure shaped by years of benchmark pressure
- highly mature data motion patterns
- narrow hot paths with minimal indirection

This matters because some FrankenSQLite regressions may not come from "bugs" in the ordinary sense. They may come from being 2x or 3x too expensive in many tiny places, which only becomes obvious once compared against SQLite.

### 10. Interaction effects across layers

The worst regressions may not belong cleanly to one subsystem.

Examples:

- a B-tree split decision can increase allocator pressure
- allocator pressure can widen the MVCC conflict surface
- a widened conflict surface can increase retries
- retries magnify the cost of page copies and restore logic
- page copies magnify the importance of VFS and allocator churn

That is why the matrix must be read as a systems problem, not as a hunt for a single bad function.

## Detailed Intervention Ledger

This section is intentionally redundant with some earlier sections. The goal is to make it very easy for an external reviewer to see what was tried, why it was tried, and whether it:

- improved the performance frontier
- merely fixed a regression
- improved correctness without changing speed
- or made performance actively worse

### Phase 1: Benchmark validity and corpus realism

What was done:

- fixed the benchmark harness to resolve the canonical pinned real-database fixtures rather than naive default paths
- expanded the corpus with larger and more realistic databases, especially Agent Mail and related real stores

Why it mattered:

- without this, benchmark results could be distorted by toy or misresolved fixtures
- realistic page layouts are necessary to expose the true structural conflict geometry

What it changed:

- improved confidence in the measurements
- did not directly improve engine speed

### Phase 2: Deep profiling of the real bad rows

What was done:

- generalized the profiling flow so it could target the actual problematic workloads rather than only a generic mixed workload
- captured `perf stat`, `perf report`, internal profile summaries, and workload-specific artifacts for the true hotspots

Why it mattered:

- this moved the work out of the realm of vague suspicion
- it established that the catastrophic row was dominated by the write path, not by the parser or idle cores

What it changed:

- materially improved diagnosis
- did not directly improve throughput

### Phase 3: Row-cache and decode-path cleanup

What was done:

- reactivated or corrected row-cache behavior in the storage cursor path
- reduced redundant decode/materialization work where possible

Why it mattered:

- mixed and single-writer rows were obviously too slow even without severe retry storms
- low-concurrency rows expose these fixed costs sharply

What it changed:

- helped baseline overhead somewhat
- did not solve the flagship concurrent-insert collapse

### Phase 4: Direct-handle and already-owned-page fast-path work

What was done:

- reduced repeated rediscovery of concurrent transaction state
- improved handling when the current transaction already owns a page
- corrected savepoint/rollback behavior so owned pages remain restageable without re-entering stale checks incorrectly

Why it mattered:

- repeated handle lookup and redundant ownership bookkeeping are exactly the kinds of hot-path costs that grow under retry pressure

What it changed:

- this is one of the few interventions that clearly improved the bad row materially
- it moved the focused `disjoint c8` row from catastrophic levels into the rough `684-859 ops/s` band on some trees
- it still left the engine behind SQLite

Category:

- real frontier improvement

### Phase 5: Page-one synthetic tracking cleanup

What was done:

- fixed stale synthetic page-one tracking and related cleanup/restore behavior

Why it mattered:

- page 1 had already been identified as a false conflict surface
- stale state here could create both correctness issues and avoidable conflict amplification

What it changed:

- improved correctness and removed at least one source of false sharing
- not enough by itself to change the overall performance story

Category:

- correctness plus likely small performance help

### Phase 6: Commit-planning regression repair

What was done:

- fixed commit-planning logic that had started looking for pending commit pages after the active transaction handle had already been detached in autocommit flows

Why it mattered:

- this bug could make benchmark runs fail or produce internal errors instead of valid measurements

What it changed:

- restored correct MVCC execution
- returned the benchmark to the old bad frontier
- did not materially improve throughput

Category:

- regression fix, not frontier improvement

### Phase 7: No-op write elision and unchanged-page preservation

What was done:

- added or reinforced logic to skip writes when a page image did not materially change
- preserved original page images more carefully through rollback and restore paths

Why it mattered:

- unnecessary rewrites enlarge the conflict surface and waste I/O/copy work

What it changed:

- reduced some avoidable churn
- directionally correct, but not enough to dominate the overall picture

Category:

- likely small real improvement

### Phase 8: Local leaf-split work

What was done:

- introduced and refined a more local `LeafTable` split path so inserts would not immediately fall into broader sibling-rebalance work
- later fixed a concrete bug where the local split path could fire when the parent lacked room

Why it mattered:

- reducing structural fan-out is one of the few ideas that attacks the problem at the correct architectural level

What it changed:

- directionally promising
- implementation proved delicate
- one version clearly worsened behavior before the parent-capacity gate fix

Category:

- promising direction, not yet validated as a strong frontier improvement

### Phase 9: Structural preclaim experiments

What was done:

- first tried preclaiming a broader structural page set
- then tried narrowing the idea to parent-only preclaim

Why it mattered:

- the hypothesis was that deterministic earlier ownership could reduce mid-operation wasted work

What it changed:

- correctness was fine
- performance got dramatically worse
- these experiments are now valuable mainly because they falsified a tempting intuition

Category:

- actively harmful idea; reverted

### Phase 10: Fresh-eyes audit passes

What was done:

- reread recent changes looking for wrong assumptions, unsafe fast paths, oversized buffer mistakes, and cleanup/reconciliation bugs

Why it mattered:

- without this, the benchmark story can get polluted by hidden correctness breakage or accidental path inflation

What it changed:

- removed several distortions
- increased confidence that the remaining slowness is genuine and systemic

Category:

- correctness and hygiene work; may remove noise from perf results

### Phase 11: Manual revert of structural preclaim

What was done:

- surgically removed the explicit structural preclaim hook and its live call sites without using destructive git rollback

Why it mattered:

- the idea had already been proven harmful
- leaving it landed would contaminate all future benchmark interpretation

What it changed:

- removed a known bad idea from the current tree
- preserved the failed artifacts as historical evidence

Category:

- cleanup of a known-bad performance regression

## What Changed The Frontier Versus What Merely Fixed Regressions

This distinction matters because the project has repeatedly had sessions where something important was fixed, but the performance frontier did not actually move.

### Clear or likely frontier improvements

- direct-handle / already-owned-page fast-path work
- some row-cache and redundant bookkeeping reductions
- no-op write elision / unchanged-page preservation

### Important fixes that mostly restored validity or correctness

- benchmark fixture-resolution and corpus repair
- commit-planning regression fix
- page-one synthetic tracking cleanup
- fresh-eyes audit fixes

### Clearly harmful ideas

- broad structural preclaim
- parent-only structural preclaim

### Still unresolved

- whether local structural-op redesign can produce a large real gain
- how much of the baseline gap is frontend versus storage-path versus data-motion cost
- how much allocator redesign can reduce the "false disjoint" problem

## Best Current Interpretation

The strongest current interpretation is:

- FrankenSQLite does **not** mainly lose because it discovers parent conflicts late.
- FrankenSQLite loses because shared structural pages exist in the first place, and once they are touched, the current machinery around them is too expensive.
- Early acquisition makes this worse because it lengthens the shared critical region.
- Therefore the right target is probably **not** "better early locking."
- The right target on the concurrent-insert side is probably "avoid shared structural mutation most of the time, and when it is unavoidable, make the shared publish window extremely short."
- But that is only half the problem. The single-writer lane and low-concurrency rows also show that the engine has a separate baseline-efficiency deficit versus SQLite.

## Most Promising Future Avenues

### 1. Reduce how often inserts need shared structural mutation at all

This is the most promising direction.

Ideas to pressure-test:

- leave more slack in leaves under concurrent-insert-heavy workloads
- redesign split thresholds for concurrency rather than compactness
- reduce eager parent rewrites
- bias toward structures that keep inserts leaf-local longer

The core idea is: the best shared-page conflict is the one that never happens.

### 2. Turn split/rebalance into an optimistic local plan plus a tiny publish window

This is still only a hypothesis now, not something I have high confidence in yet.

The promising variant would be:

- compute the new leaf/sibling images locally
- validate parent version or shape without holding it early
- acquire the parent only for a short final publish step
- abort and retry if the parent changed underneath

This is very different from the failed "preclaim early and hold" approach.

### 3. Make allocator growth more truly disjoint

This remains important.

Ideas to pressure-test:

- extent or page-range reservation
- per-writer allocator regions
- rowid or page allocation strategies that reduce shared metadata churn

This is especially relevant because page-1 and allocator metadata have already been shown to create false conflict surfaces.

### 4. Continue shaving the core write-page machinery

Even after structure-level fixes, [engine.rs:880](/data/projects/frankensqlite/crates/fsqlite-vdbe/src/engine.rs:880) still looks too expensive.

Promising targets include:

- fewer handle/lock-table/commit-index round trips per page
- less staging/copying per page
- less restore/synthetic-surface cleanup complexity in the common case
- less work for already-owned pages

### 5. Attack the baseline engine gap directly, independent of MVCC

The matrix proves this has to be a first-class track, not a side quest.

Promising targets include:

- reducing `PageData` cloning and ownership churn
- reducing VFS read / buffer normalization / page copy overhead
- reducing per-statement engine setup cost
- reducing parser / compile / record decode / row materialization overhead on mixed workloads
- reducing allocator pressure and transient heap churn in common paths

If this track is ignored, even a perfect concurrent-writer story will still leave FrankenSQLite far behind SQLite on a large chunk of the matrix.

### 6. Revisit commit/publication critical sections after the structural issue is addressed

There are still serialized regions in commit/publication paths. Those are probably not the first-order reason the showcase row fails, but they likely still matter once the bigger conflict problem is addressed.

### 7. Keep measurement focused and adversarial

Measurement is still necessary, but it should be tightly coupled to one concrete code hypothesis at a time. The failed preclaim experiments are a good example of the kind of hard falsification that is actually useful.

## My Current Recommendation

- Keep the structural preclaim experiment reverted.
- Keep the confirmed correctness fix in [connection.rs](/data/projects/frankensqlite/crates/fsqlite-core/src/connection.rs) for commit planning.
- Preserve the direct-handle / owned-page / rollback fixes that materially improved the bad row before the preclaim regression.
- Ask GPT Pro to focus on **two parallel questions**:
  - how to avoid or drastically shorten shared B-tree structural mutation for concurrent inserts
  - how to reduce the broad baseline engine cost that shows up even in single-writer and low-concurrency rows

## Questions I Would Want GPT Pro To Answer

- What concrete B-tree design changes would keep concurrent inserts leaf-local longer without breaking correctness?
- Is there a sound optimistic structural-publish scheme that avoids holding parent pages across page construction?
- How should the allocator or rowid assignment be changed so SQL-level disjoint inserts are also disjoint in page-space terms?
- What parts of [engine.rs:880](/data/projects/frankensqlite/crates/fsqlite-vdbe/src/engine.rs:880) can be radically simplified without weakening correctness?
- Which parts of the broad baseline gap versus SQLite are most likely due to page-copy/staging overhead versus planner/VDBE overhead versus pager/VFS overhead?
- Which currently shared surfaces are fundamental, and which are accidental artifacts of the present implementation?

## What GPT Pro Would Still Need To Know

If I imagine approaching this project fresh, with **only** this report and no direct code access yet, there is still a clear next layer of information I would want before committing to a major redesign. The right goal is not "more random measurement." The right goal is to obtain the **minimum additional information that would most reduce uncertainty about the highest-EV fixes**.

The missing information falls into three buckets:

- exact workload semantics
- exact physical page-touch and retry surfaces
- exact per-operation cost decomposition

Those buckets matter because this project is losing for at least two distinct reasons:

- **conflict amplification** in the concurrent-writer path
- **broad baseline inefficiency** even when contention is not the main issue

The next information requested from the codebase should be designed to separate those two effects cleanly.

### Highest-value missing information, in the order I would want it

### 1. Exact workload definitions for every benchmark family

This is the single most important missing layer.

The benchmark labels are useful, but they are still abstractions. To reason well about the results, GPT Pro would want a short, precise description of what every worker is actually doing in each family:

- how many statements per transaction
- whether each transaction contains only inserts or a read/write mix
- whether inserts are monotonic rowid inserts, random key inserts, or indexed inserts
- whether the workload touches one table or several
- whether secondary indexes are present and updated
- whether reads are point lookups, scans, or index-assisted probes
- whether "disjoint" means disjoint SQL keys only, or also disjoint rowid ranges, or disjoint pages by construction

Without that layer, it is too easy to misread the meaning of a bad matrix cell. For example, a row named `commutative_inserts_disjoint_keys` sounds like the perfect showcase for MVCC, but if those inserts still share upper B-tree pages, right-edge growth, allocator metadata, or secondary indexes, then the workload is only logically disjoint, not physically disjoint.

What I would want in the report is:

- one short subsection per workload family
- pseudo-operational transaction descriptions
- a statement of which tables and indexes participate
- a statement of whether the benchmarked SQL shape is intended to be page-disjoint, merely key-disjoint, or not disjoint at all

### 2. Per-workload page-touch summaries

The next thing GPT Pro would want is a concise mapping from logical operation to physical page classes touched.

This does **not** need to be code. It needs to be an operational summary for each workload family and concurrency tier:

- leaf data pages
- parent/internal B-tree pages
- root pages
- right-edge growth path pages
- page-1 metadata
- freelist / allocator surfaces
- commit / publish metadata surfaces
- read-side snapshot or registry surfaces

The key question is:

- when one benchmark row performs badly, which page classes are actually common conflict surfaces?

That matters because different remedies apply depending on the answer:

- if the main shared surface is the target leaf, the problem is true hot-page contention
- if the main shared surface is the parent or right edge, the problem is structural amplification
- if the main shared surface is page 1 or allocator metadata, the problem is false disjointness
- if the main shared surface is commit/publication, the problem is residual centralization

This information would let GPT Pro distinguish:

- unavoidable conflicts
- accidental implementation-induced conflicts
- false conflicts created by metadata organization

### 3. One-operation path comparison: SQLite vs FrankenSQLite single-writer vs FrankenSQLite MVCC

The report currently explains the architecture broadly, but GPT Pro would still benefit from a very short "what happens during one representative operation" narrative.

Specifically, for:

- one representative insert transaction
- one representative mixed read/write transaction

I would want a step-by-step but high-level comparison:

- what stock SQLite does
- what FrankenSQLite single-writer does
- what FrankenSQLite MVCC does

Not code. Just the logical stages.

This would reveal where FrankenSQLite pays extra work relative to SQLite:

- more planner or execution setup
- more page buffer normalization or copying
- more bookkeeping around write ownership
- more page-surface tracking
- more commit metadata handling
- more restore/retry/cleanup machinery

This kind of side-by-side path explanation is incredibly useful because it turns "FrankenSQLite is slower" into "FrankenSQLite performs these seven extra kinds of work per logical operation."

### 4. Retry-cause taxonomy, not just retry counts

The report already includes retry counts in the matrix, which is helpful, but GPT Pro would want retries broken down by **reason**.

For example:

- retry because target leaf page was already owned
- retry because parent page changed or was busy
- retry because right-edge split path converged
- retry because allocator/page-1 metadata conflicted
- retry because snapshot/version validation failed
- retry because commit/publication surface changed

This distinction matters enormously. A raw retry count of `162` tells us the system is suffering. A cause distribution would tell us where to intervene first.

If most retries come from:

- parent/internal pages, then B-tree structural redesign is the priority
- page 1 / allocator, then allocator redesign is the priority
- snapshot validation, then MVCC state model and publication ordering deserve more scrutiny
- publication surfaces, then the residual centralized publish window is likely larger than currently believed

### 5. Coarse additive cost breakdowns for representative rows

The report contains profiling interpretation, but GPT Pro would still want a more additive decomposition for a few anchor rows.

The most important anchors are:

- `commutative_inserts_disjoint_keys / c1`
- `commutative_inserts_disjoint_keys / c8`
- `hot_page_contention / c8`
- `mixed_read_write / c1`
- `mixed_read_write / c8`

For each of those, I would want an approximate budget for:

- SQL parse / compile / statement setup
- read-side cursor / decode work
- page fetch / cache / normalization
- write-page staging and bookkeeping
- B-tree structure work
- retry and waiting time
- commit / publication work

This does not need to be perfect. Even rough splits would help determine whether the major gains are most likely to come from:

- B-tree design
- MVCC write-path simplification
- allocator redesign
- data-motion reduction
- frontend execution-path reduction

### 6. Fixture structural summaries

The report already says that fixture shape matters, but GPT Pro would want a more explicit profile for each benchmark fixture:

- number of pages
- B-tree depth of the relevant tables and indexes
- approximate leaf occupancy
- right-edge growth tendency
- number of secondary indexes touched by the write workloads
- overflow-page prevalence
- freelist size and allocator state

This would help explain why some rows behave differently across:

- `frankensqlite`
- `frankentui`
- `frankensearch`

It would also help separate:

- algorithmic problems that are fixture-agnostic
- physical-layout problems that depend heavily on existing tree shape

### 7. Copy / allocation / transient-buffer accounting

One broad concern throughout this investigation has been that FrankenSQLite may simply move too many bytes and allocate too much transient memory per operation compared with stock SQLite.

So GPT Pro would want simple accounting such as:

- number of page clones per logical write
- bytes copied per transaction
- number of heap allocations per transaction
- frequency of `malloc`, `memcpy`, `memmove`, or equivalent buffer churn in hot paths
- whether page staging is reusing storage or re-allocating repeatedly

This matters especially because the matrix shows that the single-writer lane is also far behind SQLite. That strongly suggests that concurrency alone is not the whole story.

### 8. Lock-hold and wait-duration distributions

If the system is losing wall-clock time to blocking or convoying, the next useful layer is not just "which page was contended," but also:

- how long the lock or logical ownership was held
- how long other writers waited
- how many wait events occurred per successful transaction
- whether waits were many tiny bursts or fewer long stalls

This is particularly relevant because the failed preclaim experiment demonstrated that simply moving acquisition earlier can make convoying drastically worse. GPT Pro would want hard evidence about hold durations before recommending any broader acquisition strategy.

### 9. Fallback-path and compatibility-path frequency

The report explains that the current runtime is hybrid and compatibility-oriented, but GPT Pro would want to know how often the benchmark actually traverses the intended fast path versus slower transitional logic.

For the benchmarked workloads, I would want to know:

- how often execution goes through the best intended native path
- how often it falls through compatibility-oriented machinery
- whether some workloads disproportionately trigger the slower path
- whether the slower path is structural or incidental

This matters because if a large fraction of the benchmark is still flowing through non-final compatibility logic, then some of the current gap is not an indictment of the long-term architecture so much as an indictment of the current implementation path.

### 10. Commit/publication critical-section timing

The report already notes that commit/publication still contains some centralized regions. GPT Pro would want their approximate timing and frequency so it can assess whether those regions are a second-order issue or a hidden first-order one.

For example:

- duration of pending-commit-surface construction
- duration of commit-index updates
- duration of publish or version-chain publication windows
- how much time is spent under any shared lock or coordination structure during commit

Even if commit/publication is not the dominant cause of the worst `disjoint c8` collapse, it may still materially cap scalability in other rows.

### Additional context GPT Pro would want about prior work

### 11. A compressed attempt ledger with before/after row deltas

The report already contains a rich intervention history, but GPT Pro would benefit from a more compressed summary table:

- hypothesis
- subsystem touched
- representative before row
- representative after row
- result category:
  - real frontier gain
  - regression fix
  - neutral
  - harmful

That table would let a fresh reader avoid re-running dead-end ideas and quickly identify which interventions actually moved throughput versus merely restored correctness.

### 12. One explicit statement of the current authoritative tree and best-known row

A fresh reader would also want a single unambiguous statement of:

- which tree is the current source of truth
- which experiments are historical only
- what the best known focused result is on the current live path
- what the full-matrix source of truth is, even if stale

That helps prevent confusion between:

- old catastrophic rows
- temporary regressions
- reverted experiments
- current known-good but still too-slow behavior

### 13. A clear constraint list for acceptable solutions

Finally, GPT Pro would want the solution constraints stated explicitly in one place.

Examples:

- correctness must be preserved
- concurrent-writer mode must remain on by default
- no regression to SQLite-style global writer serialization
- no weakening of serializable guarantees just to win benchmark rows
- avoid accumulating compatibility or technical-debt shims

Those constraints matter because some superficially attractive performance fixes would simply amount to giving up the project’s purpose.

### Recommended order for the next information-gathering pass

If I wanted the smallest next evidence set that would most improve planning quality, I would gather the following in this exact order:

1. exact workload definitions
2. per-workload page-touch summaries
3. one-operation path comparison across SQLite / Franken single-writer / Franken MVCC
4. retry-cause taxonomy
5. fixture structural summaries
6. coarse subsystem cost breakdowns for anchor rows
7. copy / allocation accounting
8. lock-hold and wait-duration distributions
9. fallback / compatibility-path frequency
10. commit/publication timing

That list is intentionally ordered so that the earliest items answer:

- what the workload *means*
- where it *really collides physically*
- and what kind of *work inflation* is happening

Only after those are known does it make sense to dig deeper into more specialized timing and memory-motion details.

### Why this matters

The risk in a project like this is not just moving too slowly. The risk is moving in the wrong direction because the system’s failures are easy to misdiagnose:

- a workload that looks logically disjoint may not be physically disjoint
- a hot row may be dominated by waiting rather than useful work
- a single optimization that helps one path may worsen global queueing
- a fix that restores correctness may create the illusion of performance progress even when the frontier has not changed

So the point of this section is to define the **next best evidence frontier** for a fresh, high-powered reviewer. It is the shortest path from "rich but incomplete diagnosis" to "high-confidence intervention design."

## Verification Status Of The Current Experiments

Before the revert, the structural-preclaim experiments compiled and passed their own focused tests. That established that they were failed ideas, not trivial build breakage.

After the revert, the current tree has passed:

- `cargo test -p fsqlite-btree test_balance_table_leaf_local_split -- --nocapture`
- `cargo check --workspace --all-targets`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo fmt --all --check`

Those post-revert results should be treated as the source of truth for the current tree. I have not rerun the focused benchmark row after the revert yet, so the performance state after removing the preclaim experiment still needs a fresh measurement.
