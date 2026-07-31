---
summary: "RFC selecting an owner-issued opened-generation capability for writable existing-runtime opens."
read_when:
  - "Reviewing or implementing FrankenSQLite issue 308."
type: "rfc"
---

# RFC: Generation-Stable, Sidecar-Safe Existing-Runtime Open

**Status:** Proposed for review
**Date:** 2026-07-31
**Owner issue:** [frankensqlite#308](https://github.com/Dicklesworthstone/frankensqlite/issues/308)
**AK task:** 4353

## Decision requested

Adopt an owner-issued **opened database generation capability** as the only API allowed to cross main-file admission into writable pager/connection construction when exact generation binding is required.

The capability must carry the exact opened main-file handle, its opaque identity, the canonical logical database path used for sidecars, and the cooperative namespace binding. Pager/connection construction must consume that capability without reopening the main file by pathname.

This RFC decides architecture only. It does not authorize implementation, release, pin rotation, downstream integration, or activation.

## Context

FrankenSQLite already has strong pieces: handle-derived `FileIdentity`, expected-identity opens, namespace-generation binding, existing-only pager policy, canonical sidecar naming, and public expected-identity constructors. The remaining gap is composition. A caller still supplies an identity obtained elsewhere and must preserve namespace stability while the engine independently constructs the runtime.

The downstream Agent Kernel task 4195 attempted to close the gap externally. Path reopen failed exact A/B/A generation fencing; `/proc/self/fd` changed sidecar names. The invariant belongs in the engine layer that owns both main and sidecars.

## Goals

1. Bind all main-file I/O to the exact descriptor admitted by the constructor.
2. Preserve canonical sibling paths for WAL, SHM, WAL-FEC, rollback journal, and platform lock sidecars.
3. Keep one namespace-generation binding alive for construction and the resulting connection lifetime.
4. Refuse before recovery or write effects when identity or namespace proof is absent.
5. Preserve concurrent-writer MVCC defaults and existing lock/recovery semantics.
6. Give consumers one API that cannot accidentally split preflight and runtime open.
7. Provide deterministic replacement and failure-injection proof reusable across downstream consumers.

## Non-goals

- defining read-transaction snapshot markers;
- guaranteeing zero-mutation read-only opens (#140 remains separate);
- solving `VACUUM INTO` receipt CAS (#141);
- implementing portable bounded external snapshots (#307);
- serializing writers or changing MVCC policy;
- creating persistent or cross-machine file identities;
- treating arbitrary hostile same-UID filesystem mutation as solved by advisory locks without platform proof.

## Required semantics

### Opened generation capability

Introduce an opaque internal/publicly consumable shape conceptually equivalent to:

```rust,ignore
struct OpenedDatabaseGeneration<F> {
    main: F,
    identity: FileIdentity,
    canonical_path: PathBuf,
    namespace: DatabaseNamespaceBinding,
}
```

The exact names and visibility remain implementation choices. The semantic requirements are fixed:

- `main` is the same handle from which `identity` was derived;
- `canonical_path` is normalized once and is never replaced by an fd alias;
- `namespace` is bound to `identity` before pager reads, recovery, or mutation;
- the capability is non-cloneable unless clone semantics preserve single-generation ownership;
- identity is opaque and process/boot scoped, never serialized as durable authority.

### Construction flow

1. Resolve the canonical logical path once.
2. Begin namespace admission for the requested mode.
3. Open the main file once with no-follow/regular-file requirements appropriate to the backend.
4. Derive identity from that exact handle.
5. Bind namespace admission to that identity and validate the stable path.
6. Move the opened main handle, canonical path, and namespace binding into pager construction.
7. Resolve/open recovery and WAL sidecars only through the capability's canonical sidecar resolver.
8. Perform final generation validation before returning the connection.
9. Retain the namespace binding and opened main capability for connection lifetime.

There must be no later pathname open whose handle can become authoritative for main-file I/O. Read-only identity probes are permitted only where the platform requires them (currently Windows); they must be labeled non-authoritative, closed without transferring ownership, and never supplied to pager, WAL conflict validation, or recovery.

### Sidecar resolver

Sidecar construction must be centralized behind the opened-generation capability or a resolver owned by it. The resolver must:

- generate canonical sibling names;
- reject aliases and ambiguous namespace state;
- bind sidecar open/recovery decisions to the retained main generation;
- preserve existing SQLite-compatible names;
- avoid implicit `/proc/self/fd`, symlink, or hard-link bases;
- expose deterministic injection points for replacement and failure tests.

### Operation-boundary and threat contract

The connection must revalidate the namespace binding before any later operation that first creates or independently opens a recovery sidecar. Existing already-open main/sidecar handles remain authoritative for their operation.

The contract has two explicit threat classes:

1. **Cooperative namespace participants:** the generation binding is normative for the full connection lifetime. A cooperating transition cannot replace the canonical path or sidecar namespace while the connection holds its binding.
2. **Non-cooperating same-UID filesystem mutation:** retained authoritative handles prevent already-open main/sidecar I/O from switching generations, but advisory namespace locks and sampled pathname checks cannot prevent or always detect arbitrary canonical-sidecar replacement. The implementation must either provide a separately proved platform ownership primitive for a stronger guarantee or expose a typed unsupported/refusal mode. It must not claim universal rename prevention.

A/B/A tests must label which threat class they exercise. Cooperative replacement tests must prove exclusion for the connection lifetime. Raw filesystem replacement tests must prove that constructor and operation hooks either retain their already-open generation or refuse; they must not assert an impossible guarantee that an advisory lock prevents every external rename.

`PathRefreshingWalBackend::conflicts_after_generation_change` currently reopens `db_path` after construction to read main pages. The selected contract requires this path to use the retained authoritative main capability (or a capability-derived clone that preserves the same file identity), never an unconstrained pathname-selected main handle. No post-construction component may promote a validation probe into authoritative main I/O.

## API direction

Preferred public direction:

```rust,ignore
Connection::open_existing_generation_bound(path, options)
```

The engine performs admission and capability construction internally. A lower-level constructor accepting `OpenedDatabaseGeneration` may exist for composition/tests, but ordinary consumers should not be required to:

- open a separate identity descriptor;
- calculate `FileIdentity`;
- hold a caller-managed namespace lease;
- call a second pathname-based runtime constructor.

Current `open_existing_with_expected_identity` may remain for explicit advanced callers only if its weaker caller obligations stay unambiguous. It must not be represented as equivalent to the new owner-issued capability without the issue-308 proof.

## Alternatives

### A. Keep caller-managed expected identity

**Rejected as the complete contract.** It is useful and should remain compatible, but the caller can still separate identity acquisition from runtime construction and must independently maintain namespace stability.

### B. Open the database through `/proc/self/fd`

**Rejected.** It binds the main handle but changes the path used for canonical sidecars and is Linux-specific.

### C. Pre/post pathname identity checks

**Rejected.** A/B/A replacement can occur between checks, and a successful final check does not prove that an intermediate runtime handle or recovery action used the same generation.

### D. Globally serialize writers during open

**Rejected.** It weakens FrankenSQLite's defining concurrent-writer behavior and does not by itself bind filesystem generations.

### E. Owner-issued opened-generation capability

**Selected for review.** It moves the invariant to the layer that owns the main handle, namespace, pager, recovery, and sidecar names while reusing current upstream primitives.

## Backend requirements

- **Unix:** descriptor identity, no-follow regular-file open, namespace binding, canonical sidecars, deterministic rename injection.
- **Windows:** full file identity with legacy fallback rules, reparse-point refusal, lock-sidecar compatibility, replacement refusal before effect.
- **io_uring:** wrap and retain the Unix-opened main file; never independently select the main generation by pathname.
- **Memory VFS:** use process-local identity and namespace semantics, or explicitly define why canonical filesystem sidecars do not apply.
- **Custom/unsupported VFS:** implement the capability contract or return a typed unsupported/refusal result.
- **Unnamed temporary databases:** are outside this existing-runtime pathname API and must return a typed inapplicable/unsupported result if passed to it. Named temporary files follow the owning backend's ordinary file contract and require explicit tests before support is claimed.

## Validation contract

The implementation cannot be accepted without:

1. separate cooperative and raw-filesystem A/B/A schedules at every constructor boundary;
2. a post-return A/B/A schedule that forces WAL-generation refresh and conflict validation, proving `PathRefreshingWalBackend` never selects a second main generation by pathname;
3. non-empty WAL plus canonical WAL/SHM naming and checkpoint/recovery;
4. rollback journal hot/non-hot recovery and canonical naming;
5. io_uring parity;
6. Windows identity-probe tests proving probes never become authoritative pager/backend handles;
7. typed refusal/inapplicability tests for unnamed temporary and unsupported VFS inputs;
8. objective concurrent-writer overlap evidence, not stress success alone, with no serialized fallback;
9. crash and failure injection for open, bind, sidecar, recovery, validation, and cleanup;
10. no leaked namespace leases or false success after indeterminate effects;
11. exact downstream Agent Kernel evaluation showing task 4195 no longer reopens main by pathname after generation preflight.

## Migration and compatibility

- Add the new API without silently changing ordinary `Connection::open` semantics in the first slice.
- Reuse current namespace and expected-identity machinery rather than creating a second generation authority.
- Keep current expected-identity APIs with truthful documentation until consumers migrate.
- Do not rotate downstream pins until the exact revision passes owner and consumer proof.
- Do not declare #140, #141, #307, AK 4326/4340, or Softwareco Decision 86 closed by this work.

## Rollback

Before downstream adoption, rollback is a code revert plus removal of the new API. No database migration or on-disk format change is permitted in the first implementation slice.

After downstream adoption, rollback requires consumers to return to fail-closed exposure-only behavior; no consumer may revive path-only writable open.

## Decision gates

1. Independent architecture/concurrency review of this RFC.
2. Record an accepted ADR before implementation.
3. Create a separate implementation task with exact crate/test scope.
4. Prove the owner matrix on the exact implementation revision.
5. Evaluate and pin that revision separately in Agent Kernel.
6. Record an explicit task-4195 return gate before integrating the preserved Slice-D candidate.

## Open review questions

1. Should the opened-generation capability be public or entirely hidden behind a safe `Connection` constructor?
2. Which existing `PendingNamespaceOpen` operations move into capability construction without duplicating authority?
3. When must recovery sidecars be opened eagerly versus capability-resolved lazily?
4. Which platforms can add a stronger non-cooperating ownership primitive, and which must expose the documented advisory-boundary refusal mode?
5. Which unsupported backends must refuse rather than emulate a weaker identity?
