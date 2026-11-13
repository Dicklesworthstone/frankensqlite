# Primitive-Selection Decision Cards — bd-db300.5.3.2.2

> One explicit decision card per hot metadata class from
> `HOT_METADATA_INVENTORY.md`.
> This artifact is meant to be strong enough that `bd-db300.5.3.3.1`
> can implement against it without reopening the selection debate.

---

## Closure Map

| Metadata Class | Selected Primitive | Change Type | EV | Relevance |
|----------------|--------------------|-------------|----|-----------|
| M1 `PublishedPagerState` | Keep seqlock publication, remove `Condvar`, retain bounded retry | Tune current primitive | 6/10 | High |
| M2 `BoundPagerPublication` | Keep stack-copy local binding | No change | 2/10 | Medium |
| M3 schema cookie + generation | Keep per-connection scalar invalidation tokens | No change | 1/10 | Medium |
| M4 cached read snapshot | Keep per-connection parked snapshot reuse | No change | 5/10 | Medium |
| M5 cached write txn (`:memory:`) | Keep per-connection retained write txn | No change | 7/10 | High for `:memory:` |
| M6 `PagerInner` committed state | Immutable snapshot publication with generation replacement | New primitive | 8/10 | Critical |
| M7 WAL frame count + checksum state | Keep per-handle refresh model | No change | 2/10 | Medium |
| M8 WAL generation identity | Keep copy-sized per-handle identity | No change | 1/10 | Low |
| M9 MemDB visible commit seq gate | Keep local monotone staleness gate | No change | 3/10 | Medium |
| M10 cached VDBE engine | Keep per-connection engine reuse | No change | 4/10 | Medium |
| M11 concurrent registry + page-lock table | Shard the registry, keep CAS lock table fast path | New primitive for registry only | 7/10 | High |
| M12 parse cache + compiled statement cache | Keep per-connection cache; reject shared publication for now | No change | 4/10 | Medium |

The cards below all include the same closure fields:
EV, relevance, primary risk, fallback trigger, logging, verification,
user-visible failure signature, rejected alternatives, baseline comparator,
adoption wedge, rollback recipe, source status, and interference status.

---

## Card 1: M6 — `PagerInner` Committed State

**Current primitive:** `Mutex<PagerInner>` in `pager.rs`; every `pager.begin()`
and `pager.commit()` passes through it.

**Chosen primitive:** Immutable snapshot publication with generation
replacement. Writers materialize a frozen `PagerCommittedSnapshot` and publish
it atomically; readers bind the latest published snapshot without taking the
hot `PagerInner` mutex.

**Why this fits:**
- Readers need a coherent committed-state summary, not mutable access to the
  whole `PagerInner`.
- Writers already have a natural publish boundary at commit.
- Snapshot reclamation can stay simple via `Arc` ownership.
- This attacks the hottest remaining shared lock in the file-backed path.

**EV score:** 8/10

**Relevance:** Critical for c4+ file-backed workloads because it removes the
highest-value shared read-side mutex from the begin/commit path.

**Primary risk and countermeasure:**
- Risk: snapshot publication drifts from the authoritative inner state across
  DDL, freelist, or checkpoint transitions.
- Countermeasure: build the snapshot while `PagerInner` is still held, publish
  only after the committed state is finalized, and shadow-compare snapshot
  fields against the legacy read path during rollout.

**Budgeted mode and fallback trigger:**
- Target mode: immutable snapshot publication on by default after proof.
- Fallback trigger: snapshot allocation or refcount churn exceeds 5% of commit
  time, or shadow compare reports any field mismatch.
- Fallback action: force legacy mutex read path and keep publishing diagnostic
  snapshots only.

**Required logging fields:**
- `trace_id`
- `metadata_class="M6"`
- `operation=publish|read|shadow_compare`
- `snapshot_generation`
- `visible_commit_seq`
- `db_size`
- `freelist_count`
- `alloc_ns`
- `snapshot_age_commits`
- `control_mode`
- `shadow_verdict`

**Required verification:**
- Unit: concurrent read during publish never blocks on the legacy mutex.
- Unit: published snapshot matches the just-committed inner state.
- Unit: retired snapshots remain valid until the last reader drops them.
- E2E: file-backed c4 write-heavy replay shows reduced mutex wait time versus
  baseline.
- Topology: c8 cross-node run confirms refcount churn does not erase the gain.

**User-visible symptom signature and operator diagnostic cues:**
- Symptom: stale reads immediately after another connection commits.
- Diagnostic: published `snapshot_generation` or `visible_commit_seq` trails the
  live commit sequence; `shadow_verdict=diverged`.

**Rejected alternatives and why they lost:**
- Seqlock on `PagerInner` fields: `freelist` and other mutable members are not
  safe torn-read payloads.
- Left-Right: duplicates too much pager state for a relatively small summary.
- `RwLock`: adds reader-side atomic traffic without removing enough contention.
- BRAVO: wrong shape for a ~2:1 read/write surface.
- Sharding `PagerInner`: the committed state is singular, not partitionable.

**Baseline comparator:** current `Mutex<PagerInner>` read-side access during
begin/commit.

**Adoption wedge / shadow-run plan:**
- Add a control mode with `legacy`, `shadow_compare`, and `snapshot`.
- Ship `shadow_compare` first and require clean comparator runs on the canonical
  file-backed workloads before defaulting to `snapshot`.

**Rollback recipe:**
1. Force `legacy` mode.
2. Leave snapshot construction optionally enabled for diagnostics only.
3. Drain outstanding snapshots naturally via `Arc` lifetime.

**Primary source or paper status:** inventory-backed and code-indexed from
`HOT_METADATA_INVENTORY.md`; no external paper dependency is required to adopt
this primitive family.

**Interference-test requirement / status:**
- Requirement: verify interaction with WAL policy and admission control under
  mixed workloads.
- Current status: baseline file-backed rationale complete; cross-node stress is
  still required before default-on promotion.

---

## Card 2: M1 — `PublishedPagerState`

**Current primitive:** seqlock-style atomic summary with a writer-side
`publish_lock: Mutex` and `sequence_cv: Condvar`.

**Chosen primitive:** retain the seqlock summary, remove the `Condvar`, and keep
bounded retry as the read-side contract.

**Why this fits:**
- The payload is already a tiny fixed-width summary.
- Readers are retry-capable and only need coherence, not ownership.
- The expensive parts are wake/sleep choreography and auxiliary serialization,
  not the seqlock itself.

**EV score:** 6/10

**Relevance:** High for multi-connection file-backed visibility checks, but
below M6 because the core seqlock shape is already close to optimal.

**Primary risk and countermeasure:**
- Risk: replacing `Condvar` wakeups with polling burns CPU or starves a waiter
  under heavy churn.
- Countermeasure: bounded exponential backoff, striped retry counters, and a
  forced-legacy mode for shadow comparison.

**Budgeted mode and fallback trigger:**
- Target mode: `seqlock_polling`.
- Fallback trigger: polling spin budget exceeds threshold or p99 visibility wait
  regresses against the `Condvar` baseline.
- Fallback action: re-enable `Condvar` waiting while preserving the seqlock
  publication layout.

**Required logging fields:**
- `trace_id`
- `metadata_class="M1"`
- `operation=publish|read|wait`
- `sequence_value`
- `read_retry_count`
- `poll_spins`
- `wait_ns`
- `control_mode`
- `shadow_verdict`

**Required verification:**
- Unit: concurrent publish/read never surfaces torn field combinations.
- Unit: retry count stays bounded under publish churn.
- Unit: forced legacy and forced polling modes produce identical snapshots.
- E2E: c8 mixed file-backed workload shows lower wait overhead than the
  `Condvar` path.
- Topology: cross-node run measures whether polling changes tail latency.

**User-visible symptom signature and operator diagnostic cues:**
- Symptom: unusually stale visibility checks or CPU spikes during read polling.
- Diagnostic: `read_retry_count`, `poll_spins`, or `wait_ns` spike above the
  comparator baseline.

**Rejected alternatives and why they lost:**
- Arc snapshot swap: adds allocation/refcount cost to an already-small payload.
- Epoch/EBR publication: too much machinery for a retryable summary.
- Independent atomics without seqlock: permits torn cross-field reads.

**Baseline comparator:** current seqlock + `publish_lock` + `sequence_cv`
implementation.

**Adoption wedge / shadow-run plan:**
- Ship `legacy`, `shadow_compare`, and `polling` modes.
- Require stable retry counts and lower p99 wait on the canonical mixed
  workloads before promotion.

**Rollback recipe:**
1. Restore `Condvar` wait path.
2. Keep counters and shadow-compare instrumentation.
3. Preserve the seqlock read contract.

**Primary source or paper status:** inventory-backed and already represented in
code; no new external primitive research is required.

**Interference-test requirement / status:**
- Requirement: prove admission-control readers are not misled by the new wait
  distribution.
- Current status: controller-composition risk identified; replay validation
  remains mandatory before default-on.

---

## Card 3: M11 — Concurrent Registry + Page-Lock Table

**Current primitive:** global `Arc<Mutex<ConcurrentRegistry>>` for session
begin/commit/abort, plus CAS fast path in `InProcessPageLockTable`.

**Chosen primitive:** shard the registry by session id / core-locality while
keeping the existing CAS page-lock table as the ownership primitive.

**Why this fits:**
- The registry is the contended shared control-plane object.
- The page-lock table already uses the correct exact-ownership primitive.
- Sharding cuts mutex scope without introducing unsafe or complicated
  reclamation.

**EV score:** 7/10

**Relevance:** High for c8+ file-backed concurrent-writer workloads where
session lifecycle contention is material.

**Primary risk and countermeasure:**
- Risk: shard routing bugs or uneven shard hot spots create session lookup
  errors or move contention rather than removing it.
- Countermeasure: deterministic shard mapping, shard-local instrumentation, and
  global shadow verification that every routed session is found on the expected
  shard.

**Budgeted mode and fallback trigger:**
- Target mode: `sharded_registry`.
- Fallback trigger: `session_not_found`, shard skew, or commit/abort routing
  mismatch above zero tolerance.
- Fallback action: route all operations back through the legacy global mutex.

**Required logging fields:**
- `trace_id`
- `metadata_class="M11"`
- `operation=begin|commit|abort|lookup`
- `session_id`
- `shard_id`
- `lock_wait_ns`
- `active_sessions_in_shard`
- `control_mode`
- `shadow_verdict`

**Required verification:**
- Unit: deterministic shard assignment for a given `session_id`.
- Unit: begin/commit/abort on different shards never lose sessions.
- Unit: page-lock CAS semantics remain unchanged.
- E2E: c8 hot-page contention replay shows lower registry wait time versus the
  global mutex comparator.
- Topology: cross-node run checks for shard skew and routing imbalance.

**User-visible symptom signature and operator diagnostic cues:**
- Symptom: unexpected `SQLITE_BUSY` or session lookup failures during concurrent
  traffic.
- Diagnostic: non-zero `session_not_found`, shard skew above threshold, or
  `shadow_verdict=diverged`.

**Rejected alternatives and why they lost:**
- Lock-free registry with epoch reclamation: too complex for the ownership
  shape and error surface.
- `RwLock` registry: lifecycle ops are mostly writes, so reader bias does not
  help.
- Session pre-allocation: does not solve shared registry visibility or variable
  write-set ownership.

**Baseline comparator:** current single global `ConcurrentRegistry` mutex plus
existing CAS page locks.

**Adoption wedge / shadow-run plan:**
- Ship `legacy`, `shadow_compare`, and `sharded` modes.
- Promote only after comparator runs show lower lock wait without correctness
  drift.

**Rollback recipe:**
1. Force `legacy` mode.
2. Let in-flight sharded sessions drain naturally.
3. Keep shard instrumentation for postmortem analysis.

**Primary source or paper status:** inventory-backed; no external lock-free
paper is required because the selected answer is deliberate sharding, not a
novel concurrent structure.

**Interference-test requirement / status:**
- Requirement: verify interaction with admission control and hot-page routing.
- Current status: baseline rationale complete; topology-sensitive replay still
  required before graduation.

---

## Card 4: M2 — `BoundPagerPublication`

**Current primitive:** write-once local struct copied at statement entry.

**Chosen primitive:** keep the stack/local binding exactly local.

**Why this fits:** M2 is a statement-scoped bundle of M1 output, not an
independent shared publication surface.

**EV score:** 2/10

**Relevance:** Medium because it sits on the hot path, but the leverage is in
M1, not in wrapping M2 with another primitive.

**Primary risk and countermeasure:**
- Risk: turning M2 into a shared cache reuses stale visibility evidence across
  statements.
- Countermeasure: keep M2 write-once per statement and refresh from M1/M6.

**Budgeted mode and fallback trigger:**
- Target mode: unchanged local binding.
- Fallback trigger: none specific to M2; if statement-entry bind cost matters,
  optimize the upstream publication source instead.

**Required logging fields:**
- `trace_id`
- `metadata_class="M2"`
- `operation=bind`
- `visible_commit_seq`
- `read_retry_count`
- `statement_id`
- `is_memory`

**Required verification:**
- Unit: each file-backed statement binds a fresh local copy.
- Unit: no cross-statement reuse occurs.
- E2E: another connection's commit is visible after a fresh bind.
- Topology: none beyond upstream M1 checks.

**User-visible symptom signature and operator diagnostic cues:**
- Symptom: statement runs against a stale publication bind.
- Diagnostic: local `visible_commit_seq` trails the current published sequence
  at statement entry.

**Rejected alternatives and why they lost:**
- Shared `Arc` wrapper: no reuse benefit, adds lifetime coupling.
- Seqlock around local struct: no shared readers exist.
- RCU cache of bound structs: wrong scope and stale by construction.

**Baseline comparator:** current stack-copy bind path.

**Adoption wedge / shadow-run plan:** no rollout needed; keep instrumentation
only.

**Rollback recipe:** unchanged local bind remains the fallback and default.

**Primary source or paper status:** code-indexed inventory only; no external
primitive source required.

**Interference-test requirement / status:** none independent; M2 inherits M1/M6
interference behavior.

---

## Card 5: M3 — Schema Cookie + Generation

**Current primitive:** per-connection `RefCell<u32>` / `Cell<u64>` invalidation
tokens.

**Chosen primitive:** keep local scalar invalidation tokens.

**Why this fits:** readers only need cheap mismatch detection so they can
re-prepare locally; they do not need a shared schema-object publication plane.

**EV score:** 1/10

**Relevance:** Medium for correctness, low for performance.

**Primary risk and countermeasure:**
- Risk: over-engineering this into a shared publication object creates extra
  invalidation states and stale-schema failure modes.
- Countermeasure: keep scalar mismatch checks local and continue reparsing on
  mismatch.

**Budgeted mode and fallback trigger:**
- Target mode: unchanged local token checks.
- Fallback trigger: none; if reparses become expensive, optimize schema reload
  itself, not token publication.

**Required logging fields:**
- `trace_id`
- `metadata_class="M3"`
- `schema_cookie`
- `schema_generation`
- `operation=check|invalidate`
- `reprepare_triggered`

**Required verification:**
- Unit: DDL bumps the cookie/generation as expected.
- Unit: prepared statements invalidate on mismatch.
- E2E: cross-connection schema change forces reprepare.
- Topology: none.

**User-visible symptom signature and operator diagnostic cues:**
- Symptom: stale prepared statement or unnecessary reprepare churn.
- Diagnostic: mismatch counters versus prepare cache hit rate.

**Rejected alternatives and why they lost:**
- Shared schema-object RCU publication: wrong scope for the current local cache
  model.
- Seqlock token pair: unnecessary for two local scalars.
- BRAVO / lock-based schema publication: adds shared traffic without value.

**Baseline comparator:** current local scalar check at execute/query time.

**Adoption wedge / shadow-run plan:** none; the chosen primitive is the current
one.

**Rollback recipe:** current local tokens remain the permanent rollback path.

**Primary source or paper status:** inventory-backed and code-local.

**Interference-test requirement / status:** none; M3 is connection-local.

---

## Card 6: M4 — Cached Read Snapshot

**Current primitive:** per-connection parked read-only transaction handle guarded
by a local cookie.

**Chosen primitive:** keep per-connection parked snapshot reuse.

**Why this fits:** the value is already local, already profitable, and already
expresses the correct ownership model for a read snapshot.

**EV score:** 5/10

**Relevance:** Medium because it materially reduces begin overhead for read
paths, but it is not a shared publication surface.

**Primary risk and countermeasure:**
- Risk: sharing parked snapshots across connections would break ownership and
  visibility assumptions.
- Countermeasure: retain connection-local parking with stale-cookie
  invalidation.

**Budgeted mode and fallback trigger:**
- Target mode: unchanged cache-reuse path.
- Fallback trigger: cookie mismatch or any write/DDL boundary.
- Fallback action: fresh `pager.begin()`.

**Required logging fields:**
- `trace_id`
- `metadata_class="M4"`
- `operation=park|reuse|invalidate`
- `cookie_match`
- `snapshot_age_statements`
- `cache_hit`

**Required verification:**
- Unit: reuse path returns a valid read snapshot.
- Unit: stale cookie or write invalidates the parked snapshot.
- E2E: repeated read statements reuse locally without cross-connection leakage.
- Topology: none.

**User-visible symptom signature and operator diagnostic cues:**
- Symptom: missed reuse or reuse of a stale read snapshot.
- Diagnostic: `cache_hit` drops unexpectedly or stale-cookie reuse appears.

**Rejected alternatives and why they lost:**
- Shared snapshot pool: wrong ownership and reclamation model.
- RCU registry of parked read snapshots: no cross-connection sharing benefit.
- Seqlock wrapper: no shared read/write conflict exists here.

**Baseline comparator:** current local park/reuse mechanism versus always
starting a fresh read txn.

**Adoption wedge / shadow-run plan:** none beyond tracing the existing reuse
path.

**Rollback recipe:** call the existing invalidation path and fall back to fresh
begin.

**Primary source or paper status:** inventory-backed and already validated by
existing local wins.

**Interference-test requirement / status:** none independent; M4 consumes M6 and
M1 publication but does not publish shared state itself.

---

## Card 7: M5 — Cached Write Transaction (`:memory:` Fast Path)

**Current primitive:** per-connection retained write transaction guarded by a
local cookie.

**Chosen primitive:** keep retained local write-transaction reuse for `:memory:`
only.

**Why this fits:** the current path already removes a large amount of pager
ceremony and is intentionally constrained to a single-connection ownership
model.

**EV score:** 7/10

**Relevance:** High for `:memory:` throughput, but not a candidate for shared
metadata publication.

**Primary risk and countermeasure:**
- Risk: broadening this into a file-backed or shared cross-connection primitive
  would break concurrency and durability assumptions.
- Countermeasure: keep the optimization `:memory:`-only and invalidate on DDL,
  explicit `BEGIN`, or cookie mismatch.

**Budgeted mode and fallback trigger:**
- Target mode: current retained path for `:memory:`.
- Fallback trigger: stale cookie, schema change, explicit transactional mode
  change, or any non-memory backend.
- Fallback action: fresh write begin.

**Required logging fields:**
- `trace_id`
- `metadata_class="M5"`
- `operation=park|reuse|invalidate`
- `is_memory`
- `cookie_match`
- `cache_hit`

**Required verification:**
- Unit: `commit_and_retain()` reuse works on `:memory:`.
- Unit: file-backed paths never route through this optimization.
- E2E: repeated `:memory:` writes reuse without stale schema leakage.
- Topology: none.

**User-visible symptom signature and operator diagnostic cues:**
- Symptom: incorrect reuse after invalidation or missing reuse on the hot path.
- Diagnostic: reuse/park counters and cookie mismatch events.

**Rejected alternatives and why they lost:**
- Shared cross-connection write-handle pool: violates ownership and concurrency
  rules.
- RCU transaction pool: wrong lifetime model for mutable write state.
- Seqlock gating: adds shared mechanics to a local handle.

**Baseline comparator:** current retained write-txn path versus always opening a
fresh write transaction.

**Adoption wedge / shadow-run plan:** no new rollout needed; keep existing
instrumentation as the proof surface.

**Rollback recipe:** disable reuse and always open a fresh write transaction.

**Primary source or paper status:** inventory-backed and already justified by
existing measured wins.

**Interference-test requirement / status:** none; M5 is intentionally excluded
from shared publication.

---

## Card 8: M7 — WAL Frame Count + Running Checksum State

**Current primitive:** per-handle owned fields refreshed from the WAL file.

**Chosen primitive:** keep the per-handle refresh model.

**Why this fits:** the authoritative state is the WAL file plus durable snapshot
machinery elsewhere; this handle-local cache should remain cheap and private.

**EV score:** 2/10

**Relevance:** Medium for correctness and refresh cost, low as a publication
primitive candidate.

**Primary risk and countermeasure:**
- Risk: inventing a shared authoritative frame-count cache creates divergence
  from on-disk truth.
- Countermeasure: keep handle-local refresh semantics and optimize authoritative
  WAL publication separately.

**Budgeted mode and fallback trigger:**
- Target mode: unchanged per-handle refresh.
- Fallback trigger: none unique to M7; if refresh becomes dominant, address the
  upstream durable publication plane instead.

**Required logging fields:**
- `trace_id`
- `metadata_class="M7"`
- `operation=refresh|append`
- `frame_count`
- `checkpoint_seq`
- `refresh_ns`
- `rebuild_required`

**Required verification:**
- Unit: append updates local frame count and checksum correctly.
- Unit: refresh detects new frames and checkpoint resets.
- E2E: reader catches up to another writer's frames after refresh.
- Topology: file-backed mixed-reader/writer replay only.

**User-visible symptom signature and operator diagnostic cues:**
- Symptom: reader misses newly visible frames or rebuilds too often.
- Diagnostic: `rebuild_required` frequency, frame-count deltas, refresh latency.

**Rejected alternatives and why they lost:**
- Shared RCU frame index: too easy to diverge from the WAL file.
- Seqlock around local handle state: no shared readers.
- Global mutex cache: centralizes a handle-local concern.

**Baseline comparator:** current per-handle refresh model.

**Adoption wedge / shadow-run plan:** none; keep the current model and instrument
it.

**Rollback recipe:** current per-handle refresh remains the rollback path.

**Primary source or paper status:** inventory-backed and code-local.

**Interference-test requirement / status:** no independent controller
composition; topology sensitivity comes from WAL I/O, not the chosen primitive.

---

## Card 9: M8 — WAL Generation Identity

**Current primitive:** per-handle copy-sized identity
(`checkpoint_seq`, salts).

**Chosen primitive:** keep the copy-sized identity local to the handle.

**Why this fits:** this is a tiny comparison token, not a shared mutable data
structure.

**EV score:** 1/10

**Relevance:** Low for performance, moderate for correctness.

**Primary risk and countermeasure:**
- Risk: wrapping a tiny token in a shared publication primitive creates more
  invalidation machinery than payload.
- Countermeasure: keep it as a copied identity and use the existing refresh path
  when it changes.

**Budgeted mode and fallback trigger:**
- Target mode: unchanged local copy.
- Fallback trigger: if generation checks ever need a coherent multi-field
  durable bundle, piggyback through the durable SHM snapshot surface instead of
  inventing a new wrapper here.

**Required logging fields:**
- `trace_id`
- `metadata_class="M8"`
- `operation=compare|refresh`
- `checkpoint_seq`
- `salts_changed`

**Required verification:**
- Unit: checkpoint reset changes the generation identity.
- Unit: handle refresh rebuilds derived state on identity change.
- E2E: another connection's checkpoint invalidates stale local generation.
- Topology: none.

**User-visible symptom signature and operator diagnostic cues:**
- Symptom: missed checkpoint generation change or needless rebuilds.
- Diagnostic: salts mismatch counts and repeated rebuild events.

**Rejected alternatives and why they lost:**
- Seqlock pair: too much structure for a tiny local token.
- RCU object swap: same problem with added allocation.
- Left-Right / BRAVO: no shared reader population exists.

**Baseline comparator:** current local copy-and-compare behavior.

**Adoption wedge / shadow-run plan:** none.

**Rollback recipe:** current behavior is already the rollback path.

**Primary source or paper status:** inventory-backed and code-local.

**Interference-test requirement / status:** none; M8 is handle-local.

---

## Card 10: M9 — MemDB Visible Commit Seq Gate

**Current primitive:** per-connection local staleness gate.

**Chosen primitive:** keep the local monotone gate.

**Why this fits:** the gate is a cheap local check that decides whether MemDB
needs refresh; making it shared would reintroduce the very coordination it
avoids.

**EV score:** 3/10

**Relevance:** Medium because it protects correctness on the MemDB fast path.

**Primary risk and countermeasure:**
- Risk: making the gate shared turns a cheap local invalidation check into a
  shared publication surface.
- Countermeasure: keep the gate local and compare only against upstream
  authoritative publication state.

**Budgeted mode and fallback trigger:**
- Target mode: unchanged local staleness gate.
- Fallback trigger: stale detection mismatch.
- Fallback action: refresh MemDB from the authoritative publication source.

**Required logging fields:**
- `trace_id`
- `metadata_class="M9"`
- `operation=check|refresh`
- `visible_commit_seq`
- `stale_detected`
- `refresh_reason`

**Required verification:**
- Unit: stale local gate triggers refresh.
- Unit: local commits update the gate monotonically.
- E2E: remote commit invalidates local MemDB view and refreshes correctly.
- Topology: none.

**User-visible symptom signature and operator diagnostic cues:**
- Symptom: stale MemDB read after another commit.
- Diagnostic: local gate trails authoritative visible sequence.

**Rejected alternatives and why they lost:**
- Shared seqlock gate: no benefit over local compare.
- RCU global staleness object: wrong scope and added traffic.
- Lock-based shared cache: regresses the MemDB fast path.

**Baseline comparator:** current local gate plus refresh path.

**Adoption wedge / shadow-run plan:** none; retain instrumentation only.

**Rollback recipe:** current local gate is the rollback path.

**Primary source or paper status:** inventory-backed and code-local.

**Interference-test requirement / status:** none independent; M9 depends on
authoritative upstream publication, but is not a shared publication class.

---

## Card 11: M10 — Cached VDBE Engine

**Current primitive:** per-connection cached engine stored locally.

**Chosen primitive:** keep per-connection engine reuse.

**Why this fits:** the engine cache is tightly coupled to connection-local
schema state and execution ownership.

**EV score:** 4/10

**Relevance:** Medium because it affects repeated execution cost, but it should
not be turned into shared publication.

**Primary risk and countermeasure:**
- Risk: shared engine pooling introduces stale-bytecode and ownership bugs.
- Countermeasure: keep engine reuse local and invalidate on schema/token
  mismatch.

**Budgeted mode and fallback trigger:**
- Target mode: unchanged local engine reuse.
- Fallback trigger: schema mismatch or cached-engine divergence.
- Fallback action: rebuild the engine for the current connection.

**Required logging fields:**
- `trace_id`
- `metadata_class="M10"`
- `operation=reuse|rebuild|invalidate`
- `schema_generation`
- `cache_hit`
- `reset_ns`

**Required verification:**
- Unit: engine reuse works within one connection.
- Unit: schema change invalidates the cached engine.
- E2E: repeated prepared execution hits the local engine cache without
  cross-connection leakage.
- Topology: none.

**User-visible symptom signature and operator diagnostic cues:**
- Symptom: stale bytecode or unexpected recompilation churn.
- Diagnostic: cache-hit rate and invalidation reasons.

**Rejected alternatives and why they lost:**
- Shared engine pool: ownership and invalidation become much harder.
- RCU published engine images: wrong coupling with connection-local state.
- Lock-based global engine cache: adds shared traffic to the statement path.

**Baseline comparator:** current per-connection engine cache versus always
  rebuilding.

**Adoption wedge / shadow-run plan:** none.

**Rollback recipe:** disable reuse and rebuild per statement if necessary.

**Primary source or paper status:** inventory-backed and code-local.

**Interference-test requirement / status:** none; M10 is connection-local.

---

## Card 12: M12 — Parse Cache + Compiled Statement Cache

**Current primitive:** per-connection `RefCell`-backed LRU / compiled-statement
cache.

**Chosen primitive:** keep the per-connection cache and explicitly reject shared
publication at this stage.

**Why this fits:** the locality evidence is lane- and connection-shaped, and
shared compile publication would couple invalidation and ownership too early.

**EV score:** 4/10

**Relevance:** Medium because compile reuse matters, but the next question is
cache-policy evidence, not publication machinery.

**Primary risk and countermeasure:**
- Risk: a shared compile cache smears schema invalidation and hurts lane
  locality.
- Countermeasure: keep caches local until the dedicated compile-cache bead
  proves a better answer.

**Budgeted mode and fallback trigger:**
- Target mode: unchanged per-connection cache.
- Fallback trigger: poor hit rate or invalidation churn.
- Fallback action: tune cache policy locally or defer to the dedicated compile
  cache work; do not introduce shared publication here.

**Required logging fields:**
- `trace_id`
- `metadata_class="M12"`
- `operation=parse_hit|parse_miss|compile_hit|compile_miss|invalidate`
- `schema_cookie`
- `schema_generation`
- `cache_size`
- `lane_id`

**Required verification:**
- Unit: schema change invalidates affected cached statements.
- Unit: no cross-connection cache leakage occurs.
- E2E: repeated statement workloads show warm-hit reuse on a single connection.
- Topology: any lane-locality work belongs to the compile-cache track, not this
  bead.

**User-visible symptom signature and operator diagnostic cues:**
- Symptom: wrong-plan reuse or unnecessary recompilation.
- Diagnostic: compile-hit rate, invalidation rate, and schema-mismatch counts.

**Rejected alternatives and why they lost:**
- Shared compile cache: not yet justified by locality evidence.
- RCU-published AST/bytecode cache: same invalidation and ownership problem.
- Seqlock cache map: wrong fit for large cached objects.

**Baseline comparator:** current per-connection LRU versus shared-cache
proposals.

**Adoption wedge / shadow-run plan:** if `bd-db300.6.1.3` later proves a shared
cache is warranted, that lands as a separate bead with separate validation.

**Rollback recipe:** retain current per-connection cache as the permanent
fallback.

**Primary source or paper status:** inventory-backed; future shared-cache work
is intentionally deferred to its own measurement bead.

**Interference-test requirement / status:**
- Requirement: none for the current keep-local decision.
- Current status: explicit linkage noted to the compile-cache decision track;
  no shared-publication interference proof is required until that track changes
  the primitive.

---

## Rejection Log Summary

The following option families were considered across the cards and rejected
unless a future bead supplies materially new evidence:

| Primitive Family | Rejection Pattern |
|------------------|-------------------|
| Whole-surface `RwLock` / BRAVO | Too much shared reader-side bookkeeping for the actual access shapes |
| Whole-object Left-Right duplication | Memory and duplication cost do not fit these metadata surfaces |
| Seqlock everywhere | Only correct for tiny retryable summaries, not mutable ledgers or local-only state |
| Shared RCU/epoch publication for local-only classes | Adds coupling and invalidation surface without value |
| Shared compile / engine pools | Prematurely smears ownership and schema invalidation across connections |

Future work must address the specific rejection reason before reopening any of
the above families.

---

## Implementation Priority

| Priority | Metadata Class | Action | Why Next |
|----------|----------------|--------|----------|
| 1 | M6 | Immutable committed-state snapshot publication | Highest-value shared mutex removal |
| 2 | M11 | Sharded registry with comparator mode | Best remaining concurrency-control-plane win |
| 3 | M1 | Remove `Condvar`, keep seqlock | Clean tune of an already-correct summary plane |
| 4 | M2/M3/M4/M5/M7/M8/M9/M10/M12 | Keep local/current primitives and preserve their boundaries | Avoid accidental complexity while the shared hotspots are fixed |

This is the closure contract for `bd-db300.5.3.2.2`: every hot metadata class
now has an explicit selected primitive, a rejected-alternatives trail, an
operator-visible failure signature, verification obligations, rollout posture,
and rollback path.
