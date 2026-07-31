---
summary: "Accepts a pager-owned database-generation authority for sidecar-proven writable existing-runtime opens."
read_when:
  - "Implementing or reviewing FrankenSQLite issue 308."
  - "Evaluating whether a downstream consumer may use generation-bound writable open."
type: "decision"
---

# ADR-0003: Generation-Stable, Sidecar-Proven Existing-Runtime Open

**Status:** Accepted
**Date:** 2026-07-31
**Owner issue:** [frankensqlite#308](https://github.com/Dicklesworthstone/frankensqlite/issues/308)
**Agent Kernel decision:** 87
**Controlling RFC:** [`df09e95bacebb860f84c7431db5b83f23b0f63c3`](https://github.com/tryingET/frankensqlite/blob/df09e95bacebb860f84c7431db5b83f23b0f63c3/docs/design/generation-stable-sidecar-safe-vfs-open-rfc.md)
**Review synthesis:** [`efae568611679df30c7db25feb5dc5c4a57627dd`](https://github.com/tryingET/frankensqlite/blob/efae568611679df30c7db25feb5dc5c4a57627dd/docs/design/generation-stable-sidecar-safe-vfs-open-post-spine-review-synthesis.md)

## Context

FrankenSQLite can derive an opaque `FileIdentity` from an opened main-file
handle, retain that file in `SimplePager`, and coordinate a pathname namespace
through `PendingNamespaceOpen` and `DatabaseNamespaceBinding`. It also has an
expected-identity writable open.

Those pieces do not prove that one writable connection remains attached to one
complete database generation. Existing code still has paths that:

- reopen the main database by pathname after construction;
- discover or create WAL, SHM, rollback journal, FEC, history, witness, and
  parallel-WAL artifacts independently;
- treat canonical sidecar spelling as stronger evidence than it is;
- publish a replacement or independently produced main image while an existing
  pager retains the old object; or
- permit weaker public entrypoints to bypass a stronger admission claim.

A caller cannot close these gaps with a pathname preflight. An exact
`A -> B -> A` replacement may cause runtime work against B while evidence names
A. An fd alias keeps A open but changes the naming root for its sidecars.
Canonical sidecar names identify a location, not the generation that owns the
artifact.

The architecture must preserve FrankenSQLite's page-level MVCC and concurrent
writers. Global writer serialization is not an acceptable substitute for
proving generation authority.

## Decision

Adopt a private, pager-owned **`DatabaseGenerationAuthority`** as the only
internal authority permitted to cross exact-generation admission into a
writable pager and connection.

The authority owns, as one lifetime unit:

1. the exact opened main-file object;
2. the opaque identity derived from that object;
3. the canonical logical database path, used only as an artifact naming root;
4. the cooperative namespace lifetime binding;
5. the admitted or authority-created sidecar set and resolver; and
6. the backend profile and lifetime state required by pager, recovery, WAL,
   checkpoint, export/copy, and auxiliary artifact managers.

### Ownership boundary

- **`fsqlite-pager` owns authority.** The authority type, fields, minting,
  extraction, sidecar admission, and complete admission-to-pager composition
  remain private to the pager crate.
- **`fsqlite-vfs` remains primitive-only.** It supplies secure opens,
  handle-derived identity, namespace locking, filesystem qualification, and
  low-level file operations. It cannot mint generation authority.
- **`fsqlite-core` owns the supported public connection API.** The sole first
  conforming constructor is conceptually:

  ```rust,ignore
  Connection::open_existing_generation_bound(path, options)
  ```

Rust has no friend-crate visibility. `fsqlite-pager` may expose one technically
public, safe, `#[doc(hidden)]` factory solely for core-to-pager composition. The
factory performs complete admission and returns an authority-bearing pager. It
must not accept caller-assembled authority parts, expose the authority, permit
extraction, or create a second supported API commitment.

### Admission boundary

Admission has one explicit linearization point. Before it, code may resolve a
logical path, qualify the backend, acquire coordination leases, securely open
existing objects, derive identity, inspect headers, and allocate in-memory
state. Before it, code must not mutate database or recovery state.

Admission linearizes only after:

1. canonical logical path and backend qualification;
2. secure existing-only, no-follow, regular, single-link main open;
3. identity derivation from that exact retained handle;
4. cooperative namespace binding to that identity;
5. complete discovery and admission of the enabled artifact family;
6. exact sidecar provenance checks; and
7. final cooperative namespace revalidation.

Recovery and other database effects begin only after the authority exists.

### Sidecar provenance

Absence of recovery-bearing sidecars is a valid clean bootstrap. A pre-existing
recovery-bearing artifact is admissible only under an enumerated,
artifact-specific owner rule that binds it unambiguously to the exact admitted
main state. Name, format, checksum, page size, salts, or replay compatibility
alone are not sufficient provenance.

If an artifact family has no exact binding rule, the strong constructor must
support only its clean or quiescent case and return a typed pre-effect
`SidecarProvenanceAmbiguous`-class refusal for pre-existing recovery state.
Artifacts created after admission are opened, created, rotated, recovered,
cleaned, and deleted only through the authority-owned resolver.

### Main-file lineage

After admission, no component may select the authoritative main file by path.
Every authoritative main read and write must descend from the admitted handle
or from a capability-derived duplicate of the same opened object that cannot
outlive the authority. Opening a path and comparing equal identity is not a
capability-derived duplicate.

The rule includes pager I/O, recovery, checkpoint support, WAL conflict reads,
header/schema probes, export, copy, FEC/repair, background work, cancellation,
and cleanup.

### Publication and generation lifetime

The first contract does not rotate a live authority.

**Generation-changing publication** means any main-byte or main-length mutation
outside the retained authority's cache-coherent transactional,
authority-admitted recovery, or checkpoint protocol. It includes object
replacement, independently produced image installation, full-image publication,
and in-place whole or partial repair, overwrite, truncate, or extend operations.

Every cooperative route capable of such publication—including weaker
connection, pager, compatibility, async, and C entrypoints—must acquire the
same namespace-wide exclusive publication gate. That gate conflicts with every
live generation authority. Publication requested through the strong connection
must refuse before effects with `GenerationRotationUnsupported` or equivalent.

Ordinary pager writes, admitted recovery, and checkpoint remain lawful and
generation-preserving only through the retained authority's coherence protocol.
A future live rotation protocol requires a separate architecture decision.

### Threat and platform boundary

The strong contract is cooperative. It guarantees retained-handle stability and
namespace-protocol exclusion for participants that honor the protocol. It does
not claim that advisory locks or sampled checks prevent arbitrary same-UID
rename, hard-link, parent-directory, A/B/A, or sidecar substitution.

Each supported backend/filesystem profile must state and prove its exact threat
contract. Unqualified NFS, SMB/CIFS, FUSE, overlay, clustered, reconnecting,
custom, weak-identity Windows, or otherwise unsupported combinations must
return a typed refusal rather than silently downgrade.

### Outcome semantics

Every admission, recovery, and publication result belongs to exactly one class:

1. **pre-effect refusal** — no database or recovery effect began;
2. **definite completion** — the declared completion and durability boundary
   was reached; or
3. **indeterminate effect** — an effect may have begun but completion or
   durability cannot be established.

An indeterminate result must never be reported as ordinary `CannotOpen`.
Crash/cancellation recovery must converge idempotently to an explicitly allowed
pre-operation or completed post-operation state.

### Concurrency

Concurrent-writer mode remains enabled by default. Generation admission and
publication exclusion must not become a global transaction or connection lock.
Acceptance requires objective overlap of disjoint-page writers and separate
measurement of existing physical publication/durability boundaries.

## Options Considered

### 1. Caller preflight plus expected-identity open

Rejected. It leaves a gap between caller proof and owner effects, does not
establish sidecar provenance, and cannot govern post-return helper paths.

### 2. Opened-main capability without generation-owned sidecars

Rejected. It preserves main-object continuity but still allows unrelated or
stale recovery artifacts to be selected from the canonical namespace.

### 3. Public caller-constructible generation capability

Rejected. Callers could combine identity, path, main handle, namespace lease,
and sidecars from different observations. Safe conformance requires owner-only
minting from one admission protocol.

### 4. Transparent replacement with live authority rotation

Deferred. Correct rotation must atomically replace main handle, identity,
namespace binding, sidecar state, WAL backend, pager caches, and every dependent
lease. The first slice refuses generation-changing publication instead.

### 5. Pager-owned private generation authority

Accepted. Pager is the narrowest crate that already owns the retained main file,
rollback recovery, page cache, WAL backend installation, export/copy, and
publication mechanics. A hidden safe factory lets core expose one supported API
without giving VFS or callers minting authority.

## Consequences

### Positive

- Exact-generation admission becomes an owner invariant rather than a caller
  convention.
- Main-handle lineage and sidecar provenance share one lifetime boundary.
- Weaker APIs may remain available without inheriting a false conformance claim.
- Cooperative replacement is excluded for the full authority lifetime.
- Unsupported artifact, platform, and feature combinations fail closed.
- The design preserves page-level MVCC and concurrent writers.

### Costs

- Pager, VFS, core, WAL, recovery, FEC, MVCC history/witness, parallel-WAL, and
  platform code require coordinated changes.
- Existing generic errors are insufficient; new typed errors and effect-aware
  outcomes are required.
- Some pre-existing WAL/journal or advanced feature combinations may initially
  refuse until exact owner binding rules exist.
- Windows and actual io_uring require distinct proof, not extrapolation from
  Unix or fallback execution.

## Required Follow-up Artifacts

Implementation may start only after review of:

- `docs/design/generation-stable-sidecar-safe-vfs-open-design-packet.md`;
- `docs/design/generation-stable-sidecar-safe-vfs-open-implementation-plan.md`;
- `docs/design/generation-stable-sidecar-safe-vfs-open-validation-rollout-rollback.md`; and
- a separately scoped issue-308 implementation task.

Owner acceptance then requires immutable implementation-revision evidence for
the full RFC validation matrix. Downstream Agent Kernel evaluation is a
separate consumer gate.

## Non-Authorizations

This ADR records the selected architecture. It does **not** authorize:

- implementation under the ADR-authoring task;
- merge, release, or version publication;
- FrankenSQLite dependency pin rotation;
- Agent Kernel integration;
- task 4195 or 4196 resumption;
- Decision 86 progress; or
- any canary activation.
