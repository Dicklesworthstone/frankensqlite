---
summary: "Revised RFC selecting an engine-owned database-generation authority with explicit sidecar admission and refusal boundaries."
read_when:
  - "Reviewing or implementing FrankenSQLite issue 308 or Decision 87."
type: "rfc"
---

# RFC: Generation-Stable, Sidecar-Proven Existing-Runtime Open

**Status:** Revised after round-2 synthesis; proposed for re-review
**Date:** 2026-07-31
**Owner issue:** [frankensqlite#308](https://github.com/Dicklesworthstone/frankensqlite/issues/308)
**Decision:** Agent Kernel decision 87
**AK tasks:** 4353, 4358

## Decision requested

Adopt an engine-owned **database-generation authority** as the only architecture allowed to cross exact-generation admission into writable pager/connection construction.

The authority must own:

- the exact opened main-file handle;
- the opaque identity derived from that handle;
- the canonical logical database path;
- the cooperative namespace binding;
- the admitted sidecar set and sidecar resolver; and
- the lifetime state needed by pager, recovery, WAL, export/copy, and auxiliary artifact managers.

The public API for the first implementation is one safe constructor:

```rust,ignore
Connection::open_existing_generation_bound(path, options)
```

Capability construction and consumption remain crate-private and sealed. Pager/connection construction consumes authority issued by this constructor without independently reopening the main file by pathname.

The first implementation refuses operations that replace the canonical main generation while the connection remains live. Every cooperative publication route, including weaker connection and pager entrypoints, must acquire one namespace-wide exclusive publication gate that conflicts with every live generation authority. Atomic live generation-authority rotation is a separate future decision.

This RFC decides architecture only. It does not authorize implementation, release, pin rotation, downstream integration, task-4195 resumption, or activation.

## Context

FrankenSQLite already has handle-derived `FileIdentity`, expected-identity opens, cooperative namespace binding, existing-only pager policy, canonical sidecar naming, and io_uring handle reuse. The prior RFC composed these into an opened-main capability.

Adversarial review found that composition incomplete:

1. canonical naming did not prove sidecar provenance;
2. raw sidecar replacement exceeded what sampled checks could guarantee;
3. live database-image publication could leave a connection retaining the old generation;
4. authoritative pathname reopens existed outside the named WAL refresh path;
5. auxiliary artifact families were not classified; and
6. weaker public APIs or caller-constructed capabilities could bypass conformance.

The revised architecture treats the admitted database generation—not one descriptor or one pathname—as the authority unit.

## Goals

1. Bind all authoritative main-file I/O to the admitted opened object.
2. Establish cooperative sidecar membership before recovery or write effects.
3. Preserve canonical SQLite-compatible artifact names without using those names as main-file authority.
4. Keep one cooperative generation domain alive for all dependent work.
5. Refuse ambiguous sidecars, unsupported artifacts, unqualified backends, and live main replacement before database effects.
6. Preserve concurrent-writer MVCC defaults and objective writer overlap.
7. Attach conformance to one exact public constructor and call graph.
8. State raw filesystem and platform limits without claiming impossible ownership.

## Non-goals

- guaranteeing zero-mutation read-only opens;
- defining read-transaction snapshot markers;
- solving `VACUUM INTO` source/candidate receipt CAS;
- implementing portable bounded external snapshots;
- supporting live generation rotation in the first slice;
- guaranteeing protection from arbitrary non-cooperating same-UID namespace mutation;
- serializing writers or changing MVCC policy;
- creating persistent or cross-machine main-file identities; or
- making every existing open API generation-bound.

## Selected architecture

### Database-generation authority

The internal shape is conceptually equivalent to:

```rust,ignore
struct DatabaseGenerationAuthority<F> {
    main: SharedAuthoritativeFile<F>,
    identity: FileIdentity,
    canonical_path: PathBuf,
    namespace: DatabaseNamespaceBinding,
    sidecars: GenerationSidecarSet,
    backend_profile: GenerationBackendProfile,
}
```

Names and field layout remain implementation choices. The semantics are fixed:

- `main` is the same opened object from which `identity` was derived;
- `canonical_path` is the naming root for artifacts, never a later main-file selector;
- `namespace` binds the canonical path to the admitted identity for cooperative participants;
- `sidecars` records every admitted or authority-created correctness artifact;
- `backend_profile` records the exact supported threat and filesystem contract;
- the authority is not publicly constructible, serializable, or forgeable from parts; and
- authority-bearing leases cannot construct a second independent pager or change generation.

An `Arc` or equivalent may share the internal authority among pager and backends. Sharing is not independent capability cloning. Each lease refers to the same admitted main object, namespace binding, and sidecar state.

### Sole conforming public entrypoint

For the first implementation, only `Connection::open_existing_generation_bound` may claim Decision-87 conformance.

The following remain weaker and non-equivalent until separately proved to delegate exclusively to the conforming path under the same feature/backend matrix:

- ordinary `Connection::open*` and `open_existing*` methods;
- `open_existing_with_expected_identity`;
- pager-level constructors;
- async wrappers;
- compatibility-flag APIs;
- C API entrypoints; and
- schema-only or custom-VFS entrypoints.

No option, environment field, feature flag, PRAGMA, or custom backend may silently downgrade the conforming constructor. Unsupported combinations return a typed refusal.

### Capability sealing

The first implementation must not expose:

- public fields of the generation authority;
- a public constructor from `FileIdentity`, `VfsFile`, path, or namespace binding;
- a public implementable trait that can mint an authority;
- deserialization or durable reconstruction;
- conversion from a validation probe into authoritative I/O; or
- an unsafe or "advanced" bypass constructor.

Any future public lower-level capability API requires a separate architecture decision.

## Admission protocol

### Backend qualification

Before admission, the engine classifies the backend and filesystem into a reviewed profile.

A strong native profile requires:

- stable live-handle identity;
- secure no-follow regular-file opening;
- single-link enforcement where aliases would create multiple artifact namespaces;
- cooperative namespace locking with proved local-filesystem semantics;
- canonical artifact resolution; and
- typed refusal when those properties cannot be established.

NFS, SMB/CIFS, FUSE, overlay, clustered, reconnecting, or otherwise unqualified filesystems are unsupported for the strong constructor until separately reviewed and proved. Windows weak identity fallback is a distinct profile and does not inherit strong conformance automatically.

### Admission effects

Before linearization, the constructor may:

- resolve and normalize a logical path;
- acquire namespace/control leases;
- securely open the main and existing artifacts without mutation;
- derive identities and inspect headers;
- allocate in-memory state; and
- create only documented coordination records whose creation cannot mutate database contents or trigger recovery.

Before linearization it must not:

- write or truncate the main database;
- append, reset, checkpoint, or recover a WAL;
- replay, delete, or truncate a rollback journal;
- initialize or mutate SHM;
- create, delete, repair, or publish data/recovery artifacts;
- publish a replacement main generation; or
- report success after an ambiguous effect.

### Linearization sequence

1. Resolve the canonical logical path once.
2. Qualify the backend/filesystem profile.
3. Begin cooperative namespace admission.
4. Open the main once using profile-required alias and regular-file protections.
5. Derive identity from that exact handle.
6. Enforce single-link and other profile invariants.
7. Bind namespace admission to the identity.
8. Discover the complete enabled artifact family.
9. Securely open and validate every pre-existing recovery-bearing artifact required by that family.
10. Establish the admitted `GenerationSidecarSet`.
11. Revalidate the cooperative namespace generation.
12. Construct the sealed `DatabaseGenerationAuthority`.

Step 12 is the admission linearization point. Recovery and other database effects occur only after it.

A failure before step 12 is a typed pre-effect refusal except for explicitly documented coordination-record effects. A failure after recovery or another durable database effect begins must report a typed effect-aware or indeterminate result rather than ordinary `CannotOpen`.

## Cooperative sidecar provenance

### Admission establishes membership

Generation membership is established at the admission linearization point; it is not inferred merely from historical creation or canonical spelling.

The bootstrap rule is fail-closed:

1. absence of recovery-bearing sidecars is a valid clean bootstrap;
2. a pre-existing recovery-bearing sidecar is admissible only when an explicitly enumerated, artifact-specific owner rule unambiguously binds it to the exact admitted main state;
3. canonical spelling, format validity, checksum validity, page-size compatibility, or replay compatibility alone do not establish that binding; and
4. every other pre-existing recovery-bearing sidecar causes typed `SidecarProvenanceAmbiguous` refusal before recovery effects.

The first implementation must enumerate each admissible artifact rule. If no exact binding rule exists for an artifact family, the strong constructor supports only its clean/quiescent case and proves refusal of its pre-existing recovery state.

For every enabled artifact family:

- all pre-existing recovery-bearing artifacts must be discovered before linearization;
- each admitted artifact must satisfy its enumerated exact-binding rule;
- the opened object, binding witness, and role enter `GenerationSidecarSet`; and
- an artifact whose membership cannot be established causes refusal before recovery.

A sidecar not present at linearization may later be created only through the authority's resolver while the cooperative namespace binding remains active. A later pre-existing artifact discovered outside an authority-governed create/rotate path is ambiguous and must not be adopted silently.

### Generation transition invariant

Every cooperative route capable of publishing or rewriting a main image must acquire the same namespace-wide exclusive publication authority. This includes ordinary and expected-identity connections, pager-level APIs, compatibility/async/C entrypoints, VACUUM paths, and any internal validated-image publication path. Weaker APIs may remain non-conforming for exact admission, but they cannot bypass a live generation authority. Publication may proceed only when no generation authority holds the conflicting lifetime lease.

A cooperative transition from main generation A to B—or an in-place main-image rewrite that preserves file identity—may publish only after it holds that exclusive authority and has one of the following outcomes for every recovery-bearing artifact:

1. cleanly drained and retired;
2. explicitly transferred through an owner-proved transition operation; or
3. refused before B publication.

The transition must not leave an A WAL, SHM, rollback journal, FEC artifact, MVCC history, witness, or parallel-WAL segment available for ordinary admission as a B artifact.

Persistent namespace and platform lock records may survive transitions only when their format is explicitly generation-independent and their content cannot be mistaken for recovery data.

This transition invariant provides cooperative provenance by induction. Canonical naming alone does not.

### Sidecar resolver

All correctness-artifact creation, open, rotation, recovery, cleanup, and deletion must pass through a resolver owned by the generation authority. The resolver must:

- preserve canonical SQLite-compatible names;
- use profile-required no-follow and regular-file checks;
- reject unsupported aliases and ambiguous state;
- return authority-bearing sidecar leases rather than ambient paths;
- retain or rotate opened sidecar authority under the same generation domain;
- expose deterministic injection points; and
- refuse unclassified artifact families.

A pathname may be passed to low-level VFS machinery as a naming coordinate. It must not escape as independent authority.

## Main-file authority after admission

No component may independently reopen the canonical main path for authoritative I/O after admission.

This includes:

- pager reads and writes;
- recovery and checkpoint support;
- WAL conflict validation and generation refresh;
- database export and copy helpers;
- page-size, header, or schema probes;
- FEC and repair paths; and
- any background or cancellation cleanup path that reads database contents.

A capability-derived duplicate is permitted only when it is a duplicate of the admitted opened object, remains identity-bound, and cannot outlive the generation authority. Opening the path and checking that the result has an equal identity is not a capability-derived duplicate.

Platform-required identity probes are observational only. Their types and ownership flow must prevent promotion into pager, WAL, recovery, export, or copy authority.

## Live generation replacement

The first generation-bound contract does not rotate authority.

Any operation invoked through a generation-bound connection that would replace, republish, or rewrite the authoritative main image—including validated database-image publication and in-place publication that preserves file identity—must return a typed `GenerationRotationUnsupported` or equivalent before replacement effects.

Every cooperative publication route, including routes entered through weaker constructors or pager APIs, must first acquire the namespace-wide exclusive publication authority. That acquisition conflicts with every live generation authority. A weaker connection may retain its separately documented publication behavior only when no generation-bound authority is live; it cannot publish around the strong contract.

A future rotation protocol must atomically replace main handle, identity, namespace binding, admitted sidecar state, WAL backend, pager caches, and every dependent lease. It requires a separate decision and proof matrix.

## Artifact classification

The implementation plan must produce an exact feature-to-artifact inventory. The architecture classifies artifacts as follows:

### Generation-governed data and recovery artifacts

- WAL;
- SHM;
- rollback journal;
- WAL-FEC and DB-FEC;
- MVCC history sidecars;
- witness logs;
- parallel-WAL segments; and
- temporary recovery/publication files that can affect database state.

Each must use authority-owned admission and resolution. If an enabled implementation cannot do so, the generation-bound constructor refuses that feature combination.

### Generation-independent coordination artifacts

Namespace records and platform lock sidecars may be classified as generation-independent only when their owner contract proves that persistence across A-to-B transition is intentional and cannot carry recovery data from A into B. They still require secure canonical resolution.

### Inapplicable artifacts

Memory VFS may define process-local equivalents. Unnamed temporary databases are outside this pathname contract. Custom VFS backends must implement a sealed owner adapter with equivalent semantics or return typed unsupported.

No artifact is implicitly safe because it is omitted from this list.

## Threat contract

### Cooperative participants

For processes and components honoring the namespace protocol:

- one admitted generation governs the full authority lifetime;
- transitions are excluded while dependent authority is live;
- artifact creation and rotation use the authority resolver;
- stale-artifact transition is forbidden; and
- ambiguity refuses before database effects.

### Non-cooperating filesystem mutation

For arbitrary same-UID mutation:

- already-open authoritative handles remain on their opened objects;
- sampled checks may detect some path changes;
- no universal claim is made that advisory locks or checks prevent A/B/A replacement;
- future pathname-selected objects cannot be guaranteed without a stronger platform primitive; and
- profiles requiring that stronger protection must prove it or refuse.

A raw-actor test passes by demonstrating only the guarantee claimed by its profile. It must not reinterpret detection as prevention.

## Alias and directory policy

- Main and correctness artifacts use no-follow/reparse-refusing regular-file opens where the platform supports them.
- Multiple hard links are refused when they can create distinct namespace families for one object.
- Path normalization is not treated as ownership.
- The base strong profile assumes the canonical parent namespace is not being mutated by a non-cooperating actor.
- A profile claiming protection against parent replacement must retain directory authority or use an equivalent proved primitive.
- Mount, bind, drive-letter, UNC, case-folding, and reparse aliases receive explicit backend treatment; they are not assumed equivalent by string normalization.

## Concurrency and lifetime

- Concurrent-writer mode remains enabled by default.
- Namespace admission and artifact provenance must not become a global transaction or connection serialization lock.
- Objective disjoint-page writer overlap is required.
- Serialization is permitted only at explicitly documented physical publication or durability boundaries already required by the engine.
- The namespace binding and generation authority outlive every dependent pager/backend lease and in-flight io_uring operation.
- Connection close or cancellation cannot release the generation while dependent I/O remains active.

## Error contract

Every admission, recovery, and publication outcome belongs to one of three semantic classes:

1. **Pre-effect refusal:** no database or recovery effect began; documented coordination-only admission effects may have occurred.
2. **Definite completion:** the operation reached its defined completion and durability boundary and may return success.
3. **Indeterminate effect:** database or recovery effects may have begun but completion or durability cannot be established; this must never be reported as an ordinary open refusal.

After crash or cancellation, recovery must converge to an explicitly allowed valid pre-operation state or completed post-operation state and remain idempotent across repeated interruption. Exact error variant names and injection cuts remain implementation choices, but the three semantic classes do not.

The implementation must expose typed distinctions for at least:

- unsupported backend/filesystem profile;
- identity unavailable or too weak;
- alias or link ambiguity;
- namespace generation drift;
- sidecar provenance ambiguity;
- unsupported artifact family or feature combination;
- live generation rotation unsupported;
- pre-effect admission refusal; and
- indeterminate effect after database or recovery work may have begun.

Generic `CannotOpen` is not sufficient for these contract boundaries.

## Validation contract

Implementation acceptance requires:

1. deterministic admission hooks and one explicit linearization witness;
2. handle-origin instrumentation proving all authoritative main I/O descends from the admitted object;
3. cooperative A/B/A exclusion for the authority lifetime;
4. stale-sidecar A-to-B transition tests for every enabled artifact family;
5. raw main and sidecar replacement schedules with claims limited to the active backend profile;
6. non-empty WAL/SHM and rollback recovery only for artifact families with an enumerated exact-binding rule; otherwise deterministic pre-effect provenance refusal;
7. coverage of WAL-FEC, DB-FEC, MVCC history/witness, and parallel-WAL or typed unsupported refusal;
8. post-return WAL refresh, export, copy, and auxiliary-read proof;
9. namespace-wide exclusion of every cooperative publication route while any generation authority is live, plus pre-effect refusal when publication is invoked through the strong connection;
10. symlink, hard-link, reparse, parent, and filesystem-profile tests;
11. actual io_uring submission/completion proof separate from fallback;
12. Windows probe non-authority and weak-identity profile tests;
13. typed refusal for unnamed temporary and unsupported VFS inputs;
14. objective concurrent-writer overlap with no serialized fallback;
15. crash/failure injection with exact pre-operation, completed post-operation, and indeterminate-effect oracles;
16. a public API conformance matrix proving weaker entrypoints do not inherit admission conformance or bypass namespace-wide publication exclusion; and
17. exact downstream Agent Kernel dependency, feature, and exclusive result-lineage validation proving that every authoritative task-4195 execution, readback, and receipt descends from the conforming constructor with no weaker fallback.

## Migration and compatibility

- Add the safe constructor without silently changing ordinary `Connection::open` semantics in the first slice.
- Keep weaker APIs only with truthful non-equivalence documentation.
- Reuse current namespace and identity machinery; do not create a second competing generation ledger.
- No database on-disk format change is authorized by this decision. If implementation finds durable sidecar-generation metadata necessary, it must return to architecture review.
- Namespace/coordination record versioning is permitted only if separately scoped and compatibility-safe.
- Do not rotate downstream pins until the exact revision passes owner and consumer proof.
- Do not declare issues #140, #141, #307, Agent Kernel tasks 4326/4340, or Softwareco Decision 86 closed by this work.

## Rollback

Before downstream adoption, rollback is removal of the new constructor and internal authority path. No data migration is permitted.

After downstream adoption, rollback requires consumers to return to fail-closed exposure-only behavior. No consumer may revive path-only writable open as an exact-generation substitute.

## Decision gates

1. Independent re-review of this exact revised packet for architecture, runtime, platform, proof, and API soundness.
2. Controlling synthesis with explicit `ready_for_adr`, `revise_rfc`, or rejection.
3. Operator acceptance before ADR authoring.
4. Accepted ADR before implementation.
5. Separate implementation task with exact crate, feature, and test scope.
6. Owner proof on one immutable implementation revision.
7. Exact Agent Kernel pin and call-path evaluation.
8. Explicit task-4195 return authorization.

## Open review questions

1. Which artifact families have an exact owner binding rule in the first implementation, and which clean/quiescent cases must refuse pre-existing recovery state?
2. Which local filesystem profiles qualify on Unix and Windows?
3. Which coordination records are genuinely generation-independent?
4. Should a future authority-rotation protocol be pursued, or should generation-bound connections remain permanently non-rotating?
