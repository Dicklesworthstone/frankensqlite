---
summary: "Problem brief for FrankenSQLite's generation-stable, sidecar-safe writable existing-runtime open contract."
read_when:
  - "Designing or reviewing FrankenSQLite issue 308 or its downstream Agent Kernel return gate."
type: "problem"
---

# Problem: Generation-Stable, Sidecar-Safe Existing-Runtime Open

**Status:** Decision input
**Date:** 2026-07-31
**Owner issue:** [frankensqlite#308](https://github.com/Dicklesworthstone/frankensqlite/issues/308)
**AK task:** 4353

## Problem statement

FrankenSQLite does not yet expose one owner-level constructor that proves all of the following as one operation:

1. the writable runtime uses the exact main-file generation that was admitted;
2. WAL, SHM, WAL-FEC, and rollback journal paths remain canonical siblings of the logical database path;
3. no pathname reopen after generation preflight can silently select a different generation;
4. open/recovery refusal occurs before mutation when the generation or namespace is ambiguous; and
5. concurrent-writer behavior remains enabled.

The missing contract is visible when a database pathname undergoes exact `A -> B -> A` replacement between generation preflight and the runtime open. A downstream wrapper that identifies A, reopens by pathname, and validates the pathname later can mutate B while its receipts name A. Reopening A through `/proc/self/fd/<n>` binds the main file but changes the base used to derive `-wal`, `-shm`, and `-journal`, creating a different correctness failure.

This is an owner problem because the invariant crosses VFS admission, pager ownership, recovery, connection construction, and sidecar naming. A consumer cannot reconstruct it from pathname checks.

## Current state

### Downstream pinned state

Agent Kernel Decision 75 / ADR-0031 task 4195 pins FrankenSQLite commit `c3c916e857ef55bed55a18b0c8b381bd9f2884e4`. Its reconciliation packet 82 v2 records that the pinned public API has no atomic checked constructor that retains and uses the exact opened main-file capability while also preserving canonical sidecars. Task 4195 is held without integrating its unsafe candidate.

### Current upstream state

Current upstream `main` contains important prior art:

- `VfsFile::file_identity` derives identity from an opened handle;
- expected-identity VFS opens validate the object actually opened;
- `PendingNamespaceOpen` and `DatabaseNamespaceBinding` coordinate a cooperative database generation;
- pager existing-only open combines namespace admission and expected-identity checks;
- `Connection::open_existing_with_expected_identity` exposes a public checked open;
- io_uring wraps the Unix-opened file rather than independently reopening the pathname.

This is not yet the requested reusable contract. The public expected-identity API requires its caller to retain a separate descriptor and prevent namespace replacement throughout open and recovery. It does not itself acquire and carry the complete capability from the exact main-file open into connection construction.

## Known dependency classification

### Direct

- **Agent Kernel task 4195** — needs a writable generation-bound existing-runtime open with canonical WAL/SHM/journal naming before Slice D can claim generation-fenced readback and execution.

### Transitive

- **Agent Kernel task 4196** — depends on 4195 and proves replay-safe failure/concurrency behavior. It consumes the resulting contract; it is not a separate upstream API requirement.

### Related, not direct

- **Agent Kernel tasks 4326 and 4340** — concern a read-only zero-mutation observation export and a separate bounded read-transaction snapshot-marker contract. A reusable admission primitive may help, but issue #308 alone does not close those requirements.
- **Softwareco Decision 86** — is downstream of a future AK observation exporter, not directly blocked on #308.
- **Issues #140, #141, and #307** — exercise adjacent read-only, receipt-CAS, and bounded-snapshot concerns but do not govern this writable open contract.

Additional consumers must be added from their owner surfaces with their required contract type. Directional similarity is not a dependency edge.

## Threat and concurrency boundary

The contract must close replacement during construction, recovery admission, sidecar binding, and every post-construction path that could reopen authoritative main-file I/O. In particular, current upstream `PathRefreshingWalBackend::conflicts_after_generation_change` reopens `db_path` after a WAL-generation change and must be routed through the retained generation capability.

Cooperative FrankenSQLite writers must remain in one namespace-generation domain for the connection lifetime. For an arbitrary non-cooperating same-UID process, retained handles prevent already-open I/O from silently switching objects, but advisory namespace locks and sampled checks cannot prevent or always detect every rename or sidecar replacement. Stronger platform guarantees require separate proof; otherwise the API must expose the limited threat boundary or return a typed refusal.

Identity-validation probes are not authoritative runtime opens. Windows may require a temporary probe handle, but that handle must never become pager, recovery, or WAL main-file I/O. Unnamed temporary databases are outside this existing-runtime pathname contract and must be rejected explicitly.

## Non-solutions

- pre/post `stat(path)` around an ordinary connection open;
- a separate identity handle followed by an independent runtime pathname open;
- `/proc/self/fd/<n>` as the database pathname;
- calling current expected-identity APIs while leaving namespace stability as an undocumented downstream obligation;
- disabling MVCC, serializing all writers, or defaulting concurrent mode off;
- treating read-only snapshot markers as equivalent to writable open generation identity.

## Success condition

A reviewed owner contract must make it impossible for the constructor or a post-return backend refresh to use an authoritative main-file handle from a generation different from the admitted capability while canonical sidecars are attributed to that capability. It must fail before recovery or write effects when that proof cannot be established. Proof must distinguish cooperative transitions from raw filesystem replacement, exercise WAL-refresh conflict reads after return, verify probe non-authority and temporary-file refusal, and provide objective concurrent-writer overlap evidence for an exact downstream pin evaluation.
