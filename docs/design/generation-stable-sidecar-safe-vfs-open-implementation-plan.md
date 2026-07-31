---
summary: "Dependency-ordered implementation plan for the Decision 87 generation authority."
read_when:
  - "Scoping issue 308 implementation tasks or reviewing their dependencies."
  - "Checking that ADR authoring is separate from implementation."
type: "plan"
---

# Implementation Plan: Generation-Stable, Sidecar-Proven Existing-Runtime Open

**Status:** Proposed for post-ADR review; no implementation authorized
**Date:** 2026-07-31
**Owner issue:** [frankensqlite#308](https://github.com/Dicklesworthstone/frankensqlite/issues/308)
**Decision:** Agent Kernel decision 87 / [ADR-0003](../adr/0003-generation-stable-sidecar-proven-existing-runtime-open.md)
**Design:** [generation authority design packet](generation-stable-sidecar-safe-vfs-open-design-packet.md)

## 1. Execution rule

ADR authoring task 4379 must not implement issue #308. Implementation begins
only under a separately created, explicitly authorized task with exact scope.
Every child slice must preserve a compilable tree or land behind a fail-closed,
non-public internal boundary. No slice may advertise conformance before the
owner proof gate closes.

A green unit test is evidence only for its exercised schedule. It is not a
substitute for the complete immutable-revision proof pack.

## 2. Planned deliverables

The implementation revision must contain:

1. one supported `Connection::open_existing_generation_bound` constructor;
2. a private pager-owned `DatabaseGenerationAuthority`;
3. one safe hidden complete-admission core-to-pager factory;
4. a closed artifact inventory and authority-owned sidecar resolver;
5. explicit backend/filesystem qualification;
6. namespace-wide publication exclusion across strong and weaker APIs;
7. handle-derived main-I/O lineage with no pathname authority after admission;
8. typed admission/publication/effect outcomes;
9. deterministic fault and race hooks;
10. platform, io_uring, crash, concurrency, and API conformance proof; and
11. an immutable owner evidence manifest naming the exact commit and commands.

No on-disk database format change is planned. Discovery that durable generation
metadata is necessary stops implementation and returns to architecture review.

## 3. Dependency graph

```text
I0 inventory freeze + test harness contract
 |\
 | +--> I1 errors and closed options
 | +--> I2 VFS profile and namespace/publication primitives
 |          |
 |          +-------------------+
 v                              v
I3 private pager authority + admission + resolver
 |\
 | +--> I4 main-I/O lineage and helper migration
 | +--> I5 artifact-family integration/refusal
 | +--> I6 publication-route closure
 |          |          |          |
 +----------+----------+----------+
                        v
                 I7 public API and conformance matrix
                        |
                        v
                 I8 platform/crash/concurrency proof
                        |
                        v
                 I9 immutable owner closure
                        |
                        v
              C0 separate Agent Kernel consumer gate
```

`C0` is not a FrankenSQLite implementation child and cannot begin until I9 is
accepted.

## 4. Work packages

### I0 — Freeze inventory and proof harness contract

**Owner:** FrankenSQLite issue #308 implementation task

**Paths:** design docs, targeted test-support modules; no production behavior
change

Actions:

- re-run source inventory on the implementation base;
- enumerate every public writable existing-open and publication entrypoint;
- enumerate every path-derived correctness, repair, history, lock, and
  temporary artifact;
- identify existing deterministic VFS/fault hooks and missing hooks;
- define handle-origin, admission-stage, effect-stage, and writer-overlap event
  schemas;
- record the exact initial feature/backend matrix; and
- mark each artifact `clean-admit`, `exact-bind`, `generation-independent`, or
  `typed-refuse`.

Exit:

- inventory has no `unknown` family;
- a new finding either receives a classification or blocks implementation; and
- proof events have stable machine-readable fields before production changes.

### I1 — Typed errors, outcome classes, and closed options

**Owner:** `fsqlite-error`, `fsqlite-core` API surface

Actions:

- add typed distinctions required by ADR-0003;
- define a closed `GenerationBoundOpenOptions` surface;
- prevent expected identity, arbitrary VFS, caller resolver, and silent
  best-effort fallback;
- classify each return path as pre-effect, definite completion, or
  indeterminate effect; and
- add mapping/display tests without collapsing contract distinctions.

Exit:

- generic `CannotOpen` is not used at selected contract boundaries;
- indeterminate states cannot map to ordinary open refusal; and
- unsupported combinations are typed and testable.

### I2 — VFS profile and publication primitives

**Owner:** `fsqlite-vfs`

Actions:

- add explicit native backend/filesystem qualification primitives;
- preserve no-follow regular single-link main open;
- make identity probes observational-only by type and API ownership;
- evolve the existing namespace `gate`/`use` protocol into the sole
  generation-lifetime/publication exclusion surface;
- expose only primitive leases needed by pager-owned complete admission;
- enumerate and securely resolve generation-independent coordination records;
- add deterministic hooks around admission and exclusive publication; and
- add Unix and Windows profile-specific refusal tests.

Exit:

- VFS cannot mint generation authority;
- an unqualified filesystem cannot silently enter strong mode;
- a publication lease conflicts with every live shared generation lease; and
- coordination records have an explicit transition contract.

### I3 — Pager-owned authority, admission, and resolver

**Owner:** `fsqlite-pager`

Actions:

- introduce private `DatabaseGenerationAuthority` and authority leases;
- move retained main, identity, canonical naming root, namespace binding,
  backend profile, and sidecar set under that owner;
- add the safe hidden complete-admission factory;
- implement the explicit admission state machine and linearization witness;
- discover the complete enabled artifact family before linearization;
- implement `GenerationSidecarSet` and authority-owned resolver;
- prohibit caller-assembled parts and authority extraction; and
- keep the namespace lease until every dependent I/O lease ends.

Exit:

- no authority exists before all admitted artifacts pass;
- no database/recovery effect occurs before linearization;
- hidden-factory compile tests prove no caller mint/extraction surface; and
- clean bootstrap works only for explicitly supported families.

### I4 — Main-I/O lineage and helper migration

**Owner:** pager/core/WAL helper call paths

Actions:

- route pager reads/writes through the admitted main object;
- replace main-path reopen in WAL conflict refresh with authority-derived I/O;
- migrate export, copy, checkpoint support, header/schema probes, FEC/repair,
  background, close, and cancellation paths;
- permit only capability-derived duplicates that cannot outlive authority;
- separate observational path probes from authoritative handle types; and
- add origin instrumentation to every authoritative main open/read/write.

Exit:

- instrumented conforming executions show exactly one admitted main origin;
- no path-opened main handle reaches authoritative I/O; and
- post-return refresh/export/copy schedules remain on the admitted object.

### I5 — Artifact-family integration or refusal

**Owner:** pager plus owning WAL/MVCC/core modules

For each family, choose exact integration or typed refusal:

- WAL and SHM;
- rollback journal;
- WAL-FEC and its rewrite temporary;
- DB-FEC;
- history and history index;
- cold witness sidecar;
- parallel-WAL certificate, certificate handoff, and segments;
- temporary recovery/publication files; and
- any newly discovered family.

Actions:

- define artifact-specific exact pre-existing binding rules;
- otherwise allow only clean/quiescent absence;
- route open/create/rotate/recover/checkpoint/cleanup/delete through resolver;
- ensure A-to-B transition drains, transfers, or refuses every artifact before
  B publication; and
- remove conforming-path ambient `std::fs`/`host_fs` authority or refuse the
  associated feature.

Exit:

- every enabled artifact has an exact rule and race tests;
- structurally valid but unrelated artifacts refuse pre-effect; and
- unintegrated features cannot be enabled by option, environment, PRAGMA, or
  fallback.

### I6 — Close every publication route

**Owner:** pager/core/public wrappers

Actions:

- inventory object replacement, VACUUM, validated-image installation,
  repair/overwrite, truncate, extend, and compatibility helpers;
- require the one exclusive namespace publication lease for every cooperative
  out-of-authority main mutation;
- make strong-connection publication return pre-effect rotation-unsupported;
- ensure weaker entrypoints cannot publish while strong authority is live;
- preserve ordinary authority-governed writes, admitted recovery, and
  checkpoint without the exclusive publication lease; and
- attach effect-aware crash state to publication operations.

Exit:

- strong, weak, pager, async, compatibility, and C routes pass the same
  publication-exclusion matrix;
- partial out-of-protocol mutation is classified as publication; and
- no test obtains safety by globally serializing transactions or connections.

### I7 — Public constructor and conformance matrix

**Owner:** `fsqlite-core`, `fsqlite` facade, public documentation

Actions:

- add `Connection::open_existing_generation_bound` and its reviewed options;
- delegate only to the hidden complete-admission pager factory;
- document weaker APIs as non-equivalent;
- update the facade and any supported async/C surface truthfully;
- prohibit feature/env/PRAGMA downgrade; and
- add compile-time and runtime API matrix tests.

Exit:

- only the exact constructor claims Decision-87 conformance;
- symbol-to-pager call graph is exclusive and machine-tested;
- unsupported backends/features return typed refusal; and
- hidden pager factory remains safe but unsupported.

### I8 — Full owner proof

**Owner:** test/harness/platform maintainers

Actions:

- run deterministic A/B/A, sidecar substitution, stale-transition, alias,
  parent, crash, cancellation, and effect-class schedules;
- prove actual io_uring submission/completion separately from fallback;
- run Windows full-identity and weak-identity refusal profiles;
- prove objective disjoint-page writer overlap;
- run workspace format, check, clippy, and tests;
- run targeted all-feature/refusal matrices; and
- capture exact command, environment, feature, platform, result, and artifact
  hashes.

Exit:

- every row of the validation plan has immutable evidence or an explicit
  unsupported refusal proof;
- no claimed profile relies on unexecuted platform inference; and
- concurrent writers remain enabled by default.

### I9 — Immutable owner closure

**Owner:** FrankenSQLite decision owner/reviewers

Actions:

- freeze one implementation commit;
- publish a machine-readable evidence manifest tied to that commit;
- obtain independent architecture, runtime, platform, proof, and API review;
- synthesize the reviews without relabeling earlier attempts; and
- record merge/release recommendation separately from architecture acceptance.

Exit:

- owner proof identifies one exact commit and feature/platform matrix;
- there is no unresolved high-severity review finding; and
- closure explicitly states whether merge or release is authorized.

### C0 — Agent Kernel consumer evaluation

**Owner:** Agent Kernel; separate task and repository

Prerequisites:

- accepted I9 owner closure;
- exact candidate FrankenSQLite revision;
- explicit consumer task scope; and
- task 4195 still held.

Actions:

- evaluate exact dependency and features in a clean Agent Kernel candidate;
- prove every authoritative execution, readback, and receipt descends from the
  conforming constructor;
- prove no path-only or weaker fallback;
- preserve ADR-0031 generation fencing; and
- request explicit task-4195 return authorization only after consumer proof.

C0 does not rotate the installed pin, integrate a preserved candidate, or
resume task 4195 by default.

## 5. Patch sequencing and review

Recommended immutable review points:

1. **R1:** I0-I2 — contracts and primitives only;
2. **R2:** I3 — owner admission and sealing;
3. **R3:** I4-I6 — lineage, artifacts, and publication closure;
4. **R4:** I7 — supported API and conformance matrix;
5. **R5:** I8-I9 — proof-only closure on the frozen implementation revision.

Each review point must name the exact commit. A later amendment invalidates only
closure for the changed revision; it does not rewrite historical reviews.

## 6. Scope controls

The implementation task may modify only the issue-308 owner surfaces named in
its AK scope. It must not:

- implement read-only snapshot markers, `VACUUM INTO` receipt CAS, or bounded
  observation export;
- close issues #140, #141, or #307;
- change Agent Kernel;
- rotate any dependency pin;
- alter Decision 83, Decision 86, or canary units;
- disable MVCC or concurrent-writer defaults;
- add durable generation metadata without returning to architecture review; or
- claim hostile same-UID namespace protection without a proved primitive.

## 7. Stop conditions

Stop and return to review if:

- complete admission cannot be owned by pager without exposing minting;
- a required artifact lacks both an exact owner-binding rule and a reliable
  pre-effect refusal;
- an effect can begin before the selected linearization point;
- a cooperative publication route cannot join the common gate;
- main pathname reopen remains in authoritative conforming-path I/O;
- a platform cannot establish the claimed profile;
- proof requires global transaction/connection serialization;
- an outcome after possible durable effect maps to ordinary refusal;
- implementation requires a database on-disk format change; or
- consumer work is requested before immutable owner closure.

## 8. Plan acceptance gate

The plan is executable only after independent review confirms:

- work packages cover every RFC validation obligation;
- implementation is separate from task 4379;
- owner and consumer gates are distinct;
- the initial feature/platform scope is fail-closed;
- proof cannot pass through fallback, stress-only, or final-state-only proxies;
  and
- rollback and stop conditions are operational.
