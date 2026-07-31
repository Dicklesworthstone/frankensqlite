---
summary: "Immutable proof, staged rollout, stop, and rollback gates for Decision 87 implementation."
read_when:
  - "Validating an issue 308 candidate or deciding whether it may be merged, released, or consumed."
  - "Planning rollback of generation-bound open."
type: "validation-plan"
---

# Validation, Rollout, and Rollback: Generation-Stable Existing-Runtime Open

**Status:** Proposed for post-ADR review; no rollout authorized
**Date:** 2026-07-31
**Owner issue:** [frankensqlite#308](https://github.com/Dicklesworthstone/frankensqlite/issues/308)
**Decision:** Agent Kernel decision 87 / [ADR-0003](../adr/0003-generation-stable-sidecar-proven-existing-runtime-open.md)
**Implementation plan:** [generation-stable open implementation plan](generation-stable-sidecar-safe-vfs-open-implementation-plan.md)

## 1. Proof policy

Acceptance is bound to one immutable FrankenSQLite commit, exact feature set,
platform profile, toolchain, and evidence manifest.

The following are not equivalent to proof:

- source inspection without executed behavior;
- identity equality without handle-origin lineage;
- canonical naming without sidecar provenance;
- a stress run without deterministic schedule control;
- throughput without measured writer overlap;
- io_uring configuration when execution fell back;
- a final successful check that erases an earlier effect;
- Unix success extrapolated to Windows;
- a green owner test extrapolated to Agent Kernel; or
- raw DB/WAL copying validated with stock Python `sqlite3`.

Every test must state the guarantee it exercises. A raw-actor schedule may prove
retained-handle stability or bounded detection, but must not be described as
prevention unless the platform profile actually prevents the mutation.

## 2. Evidence manifest

The owner proof pack must include a machine-readable manifest containing:

- repository URL and exact commit;
- clean/dirty state and submodule/dependency revisions;
- Rust toolchain and target triple;
- OS, kernel, filesystem, mount/backend classification, and architecture;
- Cargo features and relevant environment variables;
- exact command lines and exit status;
- start/end timestamps and timeout;
- test binary and evidence artifact hashes;
- whether io_uring actually submitted/completed or fell back;
- schedule seed and deterministic hook sequence;
- authority/main/sidecar lineage identifiers;
- outcome semantic class;
- writer interval observations; and
- reviewer and synthesis references.

Partial, timed-out, aborted, or crashed commands are not automatically failures,
but their effects are indeterminate until inspected. They must not be rerun and
reported as if the first attempt never happened.

## 3. Validation matrix

### 3.1 Admission and linearization

| Case | Injection/observation | Pass condition |
| --- | --- | --- |
| Stage failure | Fail before/after every admission stage | No main/recovery/sidecar effect before authority construction; typed pre-effect refusal |
| Linearization witness | Record ordered stage events | Exactly one authority-construction event precedes all recovery/data effects |
| Namespace drift | Attempt cooperative A/B transition at every stage | Admission revalidates or refuses; no mixed authority |
| Empty/missing/malformed main | Existing-only strong open | No creation or initialization; typed refusal |
| Alias/link | Symlink, hard link, relative, case, mount/bind/reparse variants | Supported profile proves secure admission; ambiguity refuses before effects |
| Unqualified filesystem | NFS, SMB/CIFS, FUSE, overlay, clustered/reconnecting, unknown | Strong constructor returns typed unsupported; no silent local-profile assumption |

### 3.2 Main-handle lineage

Instrument every authoritative open/read/write with an origin class and
lineage ID.

Schedules:

- exact main `A -> B -> A` during admission and after return;
- WAL conflict refresh after path replacement;
- export and copy after path replacement;
- header/page-size/schema/conflict probes;
- checkpoint and recovery support;
- FEC and repair reads/writes;
- background work, close, cancellation, and in-flight I/O.

Pass:

- every authoritative event descends from the admitted main handle or a
  capability-derived duplicate;
- a pathname-opened observational probe cannot be promoted;
- already-open A I/O remains on A under raw replacement; and
- no claim exceeds the active backend profile.

### 3.3 Sidecar bootstrap and substitution

For every enabled artifact family, execute:

1. absent clean bootstrap;
2. exactly bound pre-existing artifact;
3. valid-format but unrelated artifact;
4. malformed and truncated artifact;
5. raw substitution between discovery/open/effect hooks;
6. authority-owned create and rotate;
7. cleanup failure and cancellation; and
8. stale artifact across cooperative A-to-B transition.

Pass:

- clean or explicitly bound artifacts admit;
- canonical/valid/replay-compatible but unbound artifacts return pre-effect
  provenance refusal;
- later creation uses the authority resolver;
- no A artifact is ordinarily admissible as B; and
- raw-race results are reported according to profile limits.

Required families:

- WAL and SHM;
- rollback journal;
- WAL-FEC and rewrite temporary;
- DB-FEC;
- history and history index;
- witness sidecar;
- parallel-WAL certificate, handoff, and segments;
- publication/recovery temporaries; and
- all newly discovered correctness artifacts.

A family without a proved exact rule passes only by deterministic typed refusal
before effects.

### 3.4 Publication exclusion

Invoke object replacement, validated/full-image publication, VACUUM, repair,
overwrite, truncate, and extend through:

- the strong connection;
- ordinary and expected-identity connections;
- pager-level helpers;
- schema/compatibility variants;
- supported async wrappers;
- C/public facade entrypoints; and
- internal image installers.

Pass:

- strong-connection publication refuses before effects;
- all cooperative out-of-authority mutation routes conflict with a live
  generation authority;
- an allowed transition cannot publish B until every A artifact is retired,
  transferred by a proved operation, or causes refusal;
- partial mutation counts as generation-changing publication; and
- ordinary authority-governed pager writes, admitted recovery, and checkpoint
  still succeed without taking the exclusive publication gate.

### 3.5 Effect and crash semantics

Inject failure, cancellation, and process crash at:

- admission stages;
- sidecar open/create/append/sync/rename/delete;
- rollback and WAL recovery cuts;
- checkpoint cuts;
- publication preparation and durability cuts;
- namespace record preparation/publication/finish;
- cleanup, close, and authority release; and
- crash-again during recovery.

Pass:

- each result is pre-effect refusal, definite completion, or indeterminate
  effect;
- no possible-effect state is mapped to ordinary `CannotOpen`;
- persisted state converges to an allowed pre-operation or completed
  post-operation state; and
- repeated interruption and recovery are idempotent.

### 3.6 API and sealing

Compile and runtime tests must show:

- only `Connection::open_existing_generation_bound` claims conformance;
- the hidden pager factory accepts no identity/file/binding/sidecar parts;
- authority type and fields cannot be named or extracted externally;
- no public implementable trait can mint authority;
- no serde/durable reconstruction exists;
- options, feature flags, PRAGMAs, environment, custom VFS, and wrappers cannot
  silently downgrade; and
- weaker entrypoints remain publication-gated even though admission is weaker.

### 3.7 Platform profiles

**Unix local:** prove stable identity, no-follow regular single-link open,
filesystem qualification, shared/exclusive namespace locking, parent threat
boundary, and A/B/A schedules.

**Windows:** prove 128-bit handle identity, legacy weak-identity refusal,
reparse/hard-link/case/drive/UNC handling, share/delete behavior, SHM, lock
sidecars, and observational probe non-authority.

**Linux io_uring:** require an execution witness for actual ring submission and
completion. Separately exercise forced fallback and label it fallback. Close and
cancellation retain authority until completion in both paths.

**Memory/custom/temporary:** state process-local semantics for memory; refuse
unnamed temporary and unsealed custom VFS combinations.

### 3.8 Concurrent writers

Instrumentation records transaction, page set, prepare interval, WAL/durability
interval, and commit result.

Pass:

- `concurrent_mode_default` and harness defaults remain `true`;
- at least two disjoint-page writers have objectively overlapping transaction
  or prepare intervals;
- no global connection, transaction, or file-level writer baton is introduced
  as Decision-87 safety;
- existing physical publication/durability serialization is measured
  separately; and
- low-concurrency and single-writer correctness do not regress.

### 3.9 Standard engineering gates

On the frozen candidate:

```bash
cargo fmt --check
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Run additional all-feature, target-specific, deterministic fault, crash, and
e2e commands defined by the implementation proof manifest. Standard gates are
necessary but not sufficient.

## 4. Independent review gates

Before owner acceptance, obtain exact-commit review from:

1. architecture/concurrency;
2. runtime/backend implementation;
3. platform/VFS;
4. proof/crash oracle; and
5. API/governance.

A controlling synthesis must list every input review and exact commit. Historical
review attempts remain immutable evidence and cannot be relabeled to a later
revision.

Outcomes:

- `ready_for_owner_acceptance`;
- `revise_implementation`; or
- `reject`.

Owner acceptance does not imply merge, release, downstream pin, or consumer
acceptance unless those actions are separately recorded.

## 5. Rollout stages

### Stage 0 — Documentation only

Current stage. ADR and plans may be reviewed. No implementation, release, pin,
or consumer change is active.

Exit: reviewed plan and separately authorized implementation task.

### Stage 1 — Owner candidate, unreachable by default

Implement on a bounded branch/worktree. The public constructor may exist, but
no existing API delegates to it and no default behavior changes.

Exit: targeted contract tests and R1-R4 review points pass.

### Stage 2 — Immutable owner proof candidate

Freeze one commit and run the complete matrix. No code changes are allowed
under the same evidence identity; any change creates a new candidate.

Exit: I9 owner closure with explicit merge/release recommendation.

### Stage 3 — Upstream merge/release gate

Merge and release require explicit owner action after Stage 2. Record the exact
merged commit/tag and verify it contains the reviewed candidate semantics.

Exit: immutable released or directly pinnable owner revision. No consumer has
changed yet.

### Stage 4 — Agent Kernel isolated evaluation

Under a separate Agent Kernel task, evaluate the exact owner revision in a
clean candidate installation. Do not mutate the normal installed pin or the
held task-4195 candidate.

Required consumer proof:

- exact dependency revision/features;
- exclusive use of the conforming constructor;
- no weaker fallback;
- authoritative execution, readback, and receipt lineage;
- preserved ADR-0031 generation fencing; and
- clean rollback to exposure-only behavior.

Exit: consumer review recommendation only.

### Stage 5 — Explicit consumer adoption and task-4195 return

Requires separate operator authorization. Pin rotation, integration, and task
4195 resumption are distinct actions and must have their own receipts. Task
4196 remains transitively gated.

No canary activation is part of these stages.

## 6. Runtime stop triggers

Stop the active stage immediately on:

- any authoritative main pathname reopen;
- ambiguous sidecar recovery or adoption;
- cooperative publication while authority is live;
- silent feature/backend/API downgrade;
- indeterminate effect reported as ordinary refusal;
- actual io_uring not distinguished from fallback;
- Windows weak identity treated as strong;
- concurrent writers disabled or globally serialized;
- evidence manifest/revision mismatch;
- consumer execution not exclusively descended from the conforming API;
- modification of task 4195 before its return gate; or
- any canary activation request bundled with this work.

Preserve the candidate and evidence. Do not delete or overwrite failed proof
artifacts.

## 7. Rollback

### Before downstream adoption

There is no data migration. Stop using the candidate and return to the last
reviewed owner revision. Removal or disablement of the new constructor requires
a separate reviewed change; do not silently redirect it to a weaker open.

### During isolated consumer evaluation

Restore the prior exact dependency pin and fail-closed exposure-only behavior.
Do not integrate the preserved task-4195 candidate, weaken ADR-0031, or turn a
failed strong open into path-only writable open.

### After downstream adoption

Rollback requires:

1. stop new generation-bound operations;
2. drain active authority-bearing work;
3. verify no in-flight I/O or recovery remains;
4. restore the previously recorded exact consumer pin;
5. restore fail-closed exposure-only behavior;
6. run consumer rollback validation; and
7. record task/evidence/receipt lineage.

If an operation ended with indeterminate effect, do not merely reopen using the
old pin. First run the owner-defined recovery/inspection procedure and preserve
the indeterminate evidence.

### Data boundary

No on-disk migration is authorized, so rollback must not require database
rewriting. If implementation introduces durable generation metadata or changes
artifact compatibility, rollout stops and the architecture returns to review.

## 8. Decision checklist

Before any claim of completion, answer yes with evidence:

- Is the commit immutable and exact?
- Is the feature/platform matrix exact?
- Did every authoritative main I/O event descend from one admitted handle?
- Did every artifact family integrate or refuse?
- Did A/B/A and stale-sidecar schedules run deterministically?
- Did publication exclusion cover weak and strong entrypoints?
- Did ordinary writes/recovery/checkpoint remain enabled?
- Did crash tests preserve three-state semantics?
- Did actual io_uring and Windows run rather than infer?
- Did disjoint writers objectively overlap?
- Did independent exact-commit review close?
- Is downstream adoption still separately authorized?

A single `no` means the candidate is not complete for the claimed stage.
