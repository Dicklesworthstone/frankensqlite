---
summary: "Revised problem brief for generation-stable, sidecar-proven writable existing-runtime opens."
read_when:
  - "Designing or reviewing FrankenSQLite issue 308, Decision 87, or the Agent Kernel return gate."
type: "problem"
---

# Problem: Generation-Stable, Sidecar-Proven Existing-Runtime Open

**Status:** Revised decision input after adversarial review
**Date:** 2026-07-31
**Owner issue:** [frankensqlite#308](https://github.com/Dicklesworthstone/frankensqlite/issues/308)
**Decision:** Agent Kernel decision 87
**AK tasks:** 4353, 4358

## Problem statement

FrankenSQLite does not yet expose one owner-level constructor that proves a writable existing-runtime connection belongs to one admitted database generation.

The missing invariant is broader than retaining one main-file descriptor. A database generation includes:

1. the exact opened main-file object used for authoritative main I/O;
2. the cooperative namespace generation governing the logical database path;
3. the admitted or generation-created recovery and concurrency artifacts associated with that main generation;
4. the canonical path used to name those artifacts; and
5. the lifetime and transition rules that prevent later code from selecting a different main generation by pathname.

A pathname preflight followed by an independent runtime open cannot prove this invariant. Exact `A -> B -> A` replacement can cause runtime effects on B while receipts name A. Opening through `/proc/self/fd/<n>` retains A but changes the naming base for WAL, SHM, and journals. Retaining A while independently opening canonical sidecars also remains insufficient: canonical naming proves location, not provenance.

This is an owner problem because the invariant crosses VFS admission, pager ownership, recovery, connection construction, WAL refresh, export/copy helpers, database-image publication, sidecar managers, and public API conformance. A consumer cannot reconstruct it from pathname checks.

## Correctness domains

### Main-file continuity

After admission, every authoritative main read or write must use the admitted handle or a capability-derived duplicate of that same opened object. A later identity-equal pathname open is not equivalent. This rule includes pager I/O, recovery, conflict reads, export, copy, checkpoint support, and post-return refresh paths.

### Cooperative sidecar provenance

Canonical sibling spelling does not prove that a pre-existing WAL or journal belongs to the admitted main generation. The owner contract must establish generation membership at admission and preserve it by protocol:

- absence of recovery-bearing sidecars is a valid clean bootstrap;
- a pre-existing recovery-bearing artifact is admitted only by an enumerated artifact-specific rule that binds it to the exact main state;
- canonical name, format, checksum, page-size, or replay compatibility alone are not provenance;
- cooperating routes cannot perform generation-changing publication while a generation authority is live;
- cooperating generation transitions cannot publish a replacement main while stale or ambiguous recovery artifacts remain;
- sidecars created or rotated after admission are created only through the retained generation authority; and
- ambiguity causes typed refusal before database effects.

Persistent namespace and lock records are coordination artifacts. They must be securely bound to the canonical namespace but are not themselves evidence that a WAL or journal belongs to a main generation.

### Raw filesystem mutation

A retained handle prevents already-open I/O from switching objects. Advisory locks and sampled checks do not prevent or always detect arbitrary non-cooperating same-UID rename, parent-directory replacement, or sidecar substitution.

The strong contract therefore distinguishes:

1. **cooperative participants**, for which the generation authority is normative; and
2. **raw filesystem mutation**, for which only retained-handle stability and checks at defined boundaries are promised unless a separately proved platform ownership primitive is active.

The API must not claim universal hostile namespace ownership. Environments requiring that stronger property must use a proved backend profile or receive a typed unsupported/refusal result.

### Live main-generation replacement

A connection that retains generation A cannot silently continue after **generation-changing publication**: any mutation of main-file bytes or length outside the admitted authority's cache-coherent transactional, recovery, or checkpoint protocol. This includes object replacement, independently produced images, identity-preserving full-image installation, and partial repair, overwrite, truncate, or extend operations. Every cooperative generation-changing publication route—including weaker connection and pager entrypoints—must acquire one namespace-wide exclusive publication gate that conflicts with every live generation authority. The first contract must refuse such publication invoked through a generation-bound connection before effects.

Ordinary pager writes, authority-admitted WAL or rollback recovery, and checkpoint materialization are generation-preserving only through the retained authority and its coherence protocol. They remain lawful and do not acquire the exclusive publication gate. A future separately decided protocol may atomically rotate the complete generation authority.

### Artifact coverage

The decision must classify every path-derived correctness artifact, including:

- WAL and SHM;
- rollback journal;
- WAL-FEC and DB-FEC artifacts;
- MVCC history and witness sidecars;
- parallel-WAL segments;
- namespace and platform lock sidecars; and
- temporary recovery, cleanup, and publication artifacts.

Each artifact must be governed by the generation authority, proved generation-independent, or rejected as unsupported for the strong constructor. Unclassified artifacts are authority escapes.

## Current state

### Downstream pinned state

Agent Kernel Decision 75 / ADR-0031 task 4195 pins FrankenSQLite commit `c3c916e857ef55bed55a18b0c8b381bd9f2884e4`. Its reconciliation packet 82 v2 records that the pinned public API has no atomic checked constructor retaining the admitted main-file capability while preserving canonical sidecars. Task 4195 remains held without integrating its unsafe candidate.

### Current upstream state

Current upstream contains important pieces:

- `VfsFile::file_identity` derives identity from an opened handle;
- expected-identity VFS opens validate an object actually opened;
- `PendingNamespaceOpen` and `DatabaseNamespaceBinding` coordinate a cooperative main generation;
- pager existing-only open combines namespace admission, identity checks, and recovery gates;
- canonical WAL and journal paths are derived from the logical database path; and
- io_uring wraps the Unix-opened file rather than independently reopening the main path.

These pieces do not yet form the required generation authority. Current paths still reopen the main database or independently derive/open artifacts, and the namespace record does not itself establish sidecar provenance across a cooperative generation transition.

## Known dependency classification

### Direct

- **Agent Kernel task 4195** needs a generation-bound writable existing-runtime open before Slice D can claim authoritative generation-fenced readback and execution.

### Transitive

- **Agent Kernel task 4196** consumes the resulting contract for replay-safe failure and concurrency proof. It is not a separate upstream API requirement.

### Related, not direct

- **Agent Kernel tasks 4326 and 4340** concern read-only observation export and snapshot markers.
- **Softwareco Decision 86** is downstream of a future Agent Kernel observation exporter.
- **Issues #140, #141, and #307** concern adjacent zero-mutation, receipt-CAS, and external snapshot contracts.

None substitutes for issue #308.

## Required admission model

The constructor needs one explicit linearization point. Before that point it may acquire coordination leases, open objects without mutation, derive identities, and validate the complete supported artifact set. It must not perform recovery writes, WAL append, journal replay, checkpoint, SHM initialization, data-sidecar creation/deletion, main replacement, or publication.

Admission linearizes only after:

1. canonical-path and backend qualification;
2. secure single-link main open and handle-derived identity;
3. cooperative namespace binding;
4. supported sidecar discovery, secure opening, and provenance admission;
5. final cooperative generation validation; and
6. construction of one retained database-generation authority.

Recovery and other database effects occur only after linearization. Outcomes have three semantic classes: pre-effect refusal, definite completion at the operation's declared durability boundary, or indeterminate effect when database/recovery effects may have begun but completion cannot be established. Indeterminate effects must not be reported as ordinary refusal, and crash recovery must converge to a valid pre-operation or completed post-operation state idempotently.

## Non-solutions

- pre/post `stat(path)` around an ordinary connection open;
- a separate identity handle followed by independent runtime pathname opens;
- `/proc/self/fd/<n>` as the database pathname;
- canonical sidecar naming without a sidecar-admission or transition invariant;
- assuming advisory locks defeat arbitrary same-UID namespace mutation;
- allowing a generation-bound connection to publish B while retaining authority for A;
- leaving exports, copies, FEC, MVCC history, or parallel-WAL paths outside the authority;
- exposing a public capability that callers can construct from independently acquired parts;
- treating existing expected-identity APIs as equivalent to the new constructor;
- disabling MVCC or globally serializing writers; or
- treating read-only snapshot markers as equivalent to writable generation identity.

## Success condition

A reviewed owner contract must prove, for each explicitly supported backend and threat class, that:

- authoritative main I/O descends from the admitted main handle;
- cooperative sidecar membership is established at admission or by generation-owned creation;
- every cooperative out-of-authority main mutation is excluded namespace-wide while a generation authority is live, without blocking ordinary authority-governed writes, admitted recovery, or checkpoint;
- cooperative generation replacement cannot leave stale artifacts attributed to the new main;
- pre-existing recovery artifacts without an exact owner binding rule refuse before recovery;
- unsupported artifact families, filesystems, aliases, and replacement operations refuse before database effects;
- raw-mutation claims are limited to guarantees the platform can actually provide;
- concurrent-writer behavior remains enabled with objective overlap evidence; and
- every authoritative Agent Kernel task-4195 execution, readback, and receipt descends exclusively from the conforming constructor, with no weaker fallback, before explicit return authorization.
