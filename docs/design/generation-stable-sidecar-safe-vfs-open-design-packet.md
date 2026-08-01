---
summary: "Post-ADR component, call-flow, artifact, error, and proof architecture for issue 308."
read_when:
  - "Implementing the Decision 87 generation authority."
  - "Reviewing crate ownership, artifact coverage, or public API conformance."
type: "design"
---

# Design Packet: Generation-Stable, Sidecar-Proven Existing-Runtime Open

**Status:** Post-ADR design; expected-identity/readback amendment proposed for re-review
**Date:** 2026-07-31
**Owner issue:** [frankensqlite#308](https://github.com/Dicklesworthstone/frankensqlite/issues/308)
**Decision:** Agent Kernel decision 87 / [ADR-0003](../adr/0003-generation-stable-sidecar-proven-existing-runtime-open.md)
**Controlling RFC revision:** `df09e95bacebb860f84c7431db5b83f23b0f63c3`

## 1. Purpose and authority

This packet turns the accepted architecture into implementable component
boundaries. It does not claim that the behavior exists, and it does not
authorize implementation, merge, release, downstream pinning, or task-4195
resumption.

Terms:

- **Observed** describes source present at the controlling RFC baseline.
- **Selected** is a normative requirement of ADR-0003.
- **Planned** is a proposed implementation shape that may change without
  weakening the selected contract.
- **Proof** is evidence that must be produced on one immutable implementation
  revision.

## 2. Current observed topology

| Layer | Observed source | Current capability | Decision-87 gap |
| --- | --- | --- | --- |
| Public API | `crates/fsqlite-core/src/connection.rs`, `Connection::open_existing*` | Existing-only and caller-supplied expected-identity opens | No owner-minted generation authority; weaker variants can look equivalent |
| Core/pager bridge | `PagerBackend::open_existing_with_page_buffer_max`; `SimplePager::open_for_connection_with_cx_and_page_buffer_max` | Hidden connection-oriented pager bootstrap seam | Seam does not perform complete sidecar admission |
| Pager main | `crates/fsqlite-pager/src/pager.rs`, `SimplePager`, `PagerInner::db_file` | Retains the opened main object | Authority is spread across independent fields and helpers |
| Namespace | `crates/fsqlite-vfs/src/namespace.rs`, `PendingNamespaceOpen`, `DatabaseNamespaceBinding` | Shared lifetime lease and exclusive generation transition | Identity ledger does not itself bind recovery artifacts |
| Identity | `crates/fsqlite-vfs/src/traits.rs`, `FileIdentity`, `VfsFile::file_identity` | Handle-derived opaque identity | Equality does not prove handle lineage |
| Secure native open | `crates/fsqlite-vfs/src/lib.rs`, native VFS implementations | No-follow/single-link primitives and expected-identity open | Strong-profile qualification is not one explicit contract |
| Rollback recovery | `SimplePager::recover_rollback_journal_for_open` and related journal paths | Existing recovery flow | Canonical `-journal` path is not exact generation provenance |
| WAL refresh | `crates/fsqlite-core/src/wal_adapter.rs`, `PathRefreshingWalBackend` | Refreshes replaced path-visible WAL | Reopens the main path in conflict validation and independently adopts WAL |
| Main export/copy | `SimplePager::export_database_bytes`, `copy_database_to` | Export and copy helpers | Source may be reopened by pathname |
| Main publication | `SimplePager::publish_validated_database_image`; VACUUM caller | Validated image publication | Can split retained A authority from path-visible B |
| io_uring | `crates/fsqlite-vfs/src/uring.rs` | Wraps Unix-opened file | Fallback tests are not proof of actual submission/completion lineage |

This table is orientation, not proof. The implementation task must update the
inventory if source has moved.

## 3. Selected component model

```text
fsqlite-core
  Connection::open_existing_generation_bound(path, options)
       |
       | sole supported conforming API
       v
fsqlite-pager
  #[doc(hidden)] safe complete-admission factory
       |
       +-- private DatabaseGenerationAuthority
       |     +-- retained authoritative main object
       |     +-- handle-derived identity
       |     +-- canonical naming root
       |     +-- namespace lifetime/publication lease
       |     +-- GenerationSidecarSet + resolver
       |     +-- qualified backend profile
       |
       +-- authority-bearing SimplePager
               +-- recovery/checkpoint
               +-- WAL backend
               +-- export/copy
               +-- repair/FEC hooks
               +-- cache and in-flight I/O leases
       ^
       |
fsqlite-vfs primitive-only services
  secure open | identity | namespace locks | filesystem profile | file I/O
```

### 3.1 Pager-owned private authority

The planned shape is conceptual:

```rust,ignore
struct DatabaseGenerationAuthority<F> {
    main: SharedAuthoritativeFile<F>,
    identity: FileIdentity,
    canonical_path: PathBuf,
    namespace: Arc<DatabaseNamespaceBinding>,
    sidecars: GenerationSidecarSet<F>,
    backend_profile: GenerationBackendProfile,
}
```

The exact fields may differ, but the following are fixed:

- the authority is private to `fsqlite-pager`;
- identity is derived from `main`, not supplied independently;
- the canonical path is a naming coordinate, never later main authority;
- sidecar leases cannot outlive the authority;
- `Arc` cloning shares one authority and cannot mint an independent pager;
- no serialization, deserialization, extraction, or construction from parts;
- no public implementable trait can mint authority;
- connection close waits until in-flight backend operations release their
  authority leases; and
- pager may project the actual identity as a non-optional live opaque
  `AdmittedMainIdentity` readback, without exposing handles, leases, resolvers,
  minting operations, or authority extraction.

`SimplePager` should hold one authority value instead of separately exposing
main, path, namespace, and sidecar ownership. Existing fields may be migrated in
bounded steps, but the final conforming call graph must have one owner.

### 3.2 Safe hidden composition seam

The pager crate may expose one safe `#[doc(hidden)]` factory. A representative
shape is:

```rust,ignore
#[doc(hidden)]
pub async fn open_existing_generation_bound_for_connection(
    cx: &Cx,
    vfs: NativeVfs,
    path: &Path,
    options: PagerGenerationBoundOpenOptions,
) -> Result<SimplePager<NativeVfs>, GenerationOpenError>;
```

The final signature is an implementation choice. Its API proof must show:

- it accepts no opened file, namespace binding, sidecar set, resolver,
  authority lease, or other caller-assembled minting part;
- it may accept required explicit `GenerationExpectation` only to compare
  `Exact(ExpectedMainIdentityGuard)` with identity derived from the retained
  handle while the guard's originating preflight object remains live;
- it performs the whole admission protocol internally;
- its return type exposes neither authority nor extraction;
- direct use still preserves all runtime invariants; and
- only the core `Connection` constructor carries the supported conformance
  promise.

### 3.3 Sole supported public API

The selected public symbol and conceptual result contract are:

```rust,ignore
pub struct ExpectedMainIdentityGuard {
    identity: FileIdentity,
    observation: OpaqueObservedMainGuard,
}
pub struct AdmittedMainIdentity(FileIdentity);

pub enum GenerationExpectation {
    Any,
    Exact(ExpectedMainIdentityGuard),
}

pub struct GenerationBoundOpenOptions {
    pub expectation: GenerationExpectation,
    // Other fields are closed, reviewed profile settings only.
}

pub struct GenerationBoundConnection {
    connection: Connection,
    admitted_main_identity: AdmittedMainIdentity,
}

Connection::open_existing_generation_bound(path, options)
    -> Result<GenerationBoundConnection, GenerationOpenError>
```

Exact naming and accessors remain implementation choices. The semantic
requirements—purpose-specific opaque wrappers, required explicit `Any` versus
`Exact` selection with no default, and atomic mandatory successful readback—do
not. `ExpectedMainIdentityGuard` must be produced from the consumer's retained
live preflight object through an owner-reviewed comparison-only primitive. It
owns or duplicates an opaque observation guard, is consumed by `Exact`, and is
retained through public success or final failure. It exposes no raw handle,
bytes, identity extraction, `Clone`, `Copy`, or conversion from detached
`FileIdentity`. Consumer proof must show the guard and durable database identity
came from the same retained preflight object. Tests close the original caller
reference before and during open, force platform identity-reuse attempts, and
prove the owner-held guard preserves the lifetime premise.

The first options surface must be closed and explicit. It may select only
reviewed backend/runtime settings plus required explicit
`GenerationExpectation`. `Exact(ExpectedMainIdentityGuard)` is the optional
expected-generation precondition; `Any` does not select a caller-authorized
generation. Agent Kernel authoritative work must use `Exact`. It must not
accept a custom VFS, compatibility downgrade, caller artifact resolver, opened
main-authority handle, namespace lease, or option that turns unsupported into
best-effort behavior. The opaque observation guard inside `Exact` is permitted
only as a non-I/O comparison precondition.

Successful construction atomically returns the connection and a non-optional
`AdmittedMainIdentity` projected from the constructed private authority. The
readback must not echo the expected input. Both purpose-specific identity values
are live opaque equality values. The expected guard and connection-bound
admitted readback expose no `Clone`, `Copy`, serde/bytes/durable codecs, detached
raw-identity conversion, weaker-open conversion, reusable durable admission
authority, or authority construction/extraction. Existing owner-internal
`FileIdentity` namespace encoding remains internal and is not a public token
codec.

Ordinary `open*`, expected-identity, schema-only, compatibility, async wrapper,
C, and pager constructors remain non-conforming until a later reviewed change.
They must not claim equivalence. Separately, no cooperative writable API may
bypass publication exclusion while a strong authority is live.

## 4. Admission state machine

```text
Start
  -> canonical logical path resolved once
  -> backend/filesystem profile qualified
  -> cooperative admission locks acquired
  -> main securely opened existing-only exactly once
  -> identity derived from retained handle
  -> Exact expected-main identity compared [MISMATCH = PRE-EFFECT REFUSAL]
  -> alias/link invariants checked
  -> namespace bound to identity
  -> enabled artifact family enumerated
  -> pre-existing artifacts securely opened and provenance-checked
  -> namespace generation revalidated
  -> DatabaseGenerationAuthority constructed  [LINEARIZATION]
  -> admitted-main identity projected internally from authority
  -> recovery / WAL bootstrap / schema work completes with effect-aware errors
  -> connection + non-optional readback returned atomically on public success
```

### 4.1 Permitted pre-linearization effects

- path resolution and metadata inspection;
- namespace/control lock acquisition;
- secure non-mutating open of existing main and artifacts;
- handle-derived identity and header inspection;
- in-memory allocation; and
- documented generation-independent coordination record creation only when it
  cannot mutate database contents or trigger recovery.

### 4.2 Forbidden pre-linearization effects

- main write, truncate, extend, repair, or publication;
- WAL append, reset, checkpoint, or recovery;
- rollback journal replay, deletion, or truncation;
- SHM initialization or mutation;
- generation-governed artifact create/delete/repair; and
- reporting ordinary refusal after an ambiguous effect.

Each stage needs a deterministic hook. The proof harness must be able to pause
or fail before and after every stage.

Before exact comparison, only enumerated generation-independent namespace/lock
coordination records may be created or updated. They cannot carry recovery data
or admit a generation-governed sidecar, and mismatch cleanup must be idempotent.
No WAL, SHM, journal, FEC, history, witness, parallel-WAL, publication, or other
generation-governed artifact may be mutated before comparison.

## 5. Sidecar model

### 5.1 GenerationSidecarSet

The authority owns a closed, typed set. Each member records:

- artifact family and role;
- canonical naming coordinate;
- opened object or authority-governed create reservation;
- exact binding witness or clean-bootstrap creation lineage;
- backend/profile requirements;
- lifecycle state; and
- effect state for recovery or cleanup.

Ambient `PathBuf` values may reach low-level VFS calls as coordinates, but they
must not escape as independent recovery authority.

### 5.2 Admission rules

For every enabled family, the implementation inventory must select exactly one:

1. **clean absence** — absence is valid and later creation is authority-owned;
2. **exact pre-existing binding** — an enumerated owner rule proves membership;
3. **generation-independent coordination** — persistence across A/B transitions
   is explicitly proved and cannot carry recovery data; or
4. **typed unsupported/refusal** — no effects occur.

Checksums, headers, salts, page sizes, and replay compatibility may participate
in an exact rule but cannot alone establish provenance.

### 5.3 Observed feature-to-artifact inventory

The following inventory is derived from the RFC baseline. `Required first-slice
action` is normative; it does not assert implementation.

| Family | Observed naming/source | Classification | Required first-slice action |
| --- | --- | --- | --- |
| Main DB | canonical path; pager `db_file` | authoritative main | Retain one securely opened object; no post-admission path selection |
| WAL | `<db>-wal`; pager/core WAL paths | generation-governed recovery | Exact owner binding rule or refuse non-empty/pre-existing recovery state; all later opens through resolver |
| SHM | `<db>-shm`; Unix/Windows VFS | generation-governed concurrency/recovery | Admit with WAL/main domain or refuse; no pre-linearization initialization |
| Rollback journal | `<db>-journal`; pager recovery | generation-governed recovery | Exact hot/non-hot binding rule or deterministic provenance refusal before replay |
| WAL-FEC | `<db>-wal-fec`; `fsqlite-wal/src/wal_fec.rs` | generation-governed repair | Resolver integration plus exact WAL binding, otherwise typed unsupported |
| DB-FEC | `<db>-fec`; `fsqlite-core/src/db_fec.rs` | generation-governed repair | Eliminate ambient `host_fs` authority on conforming path or refuse feature |
| History | `<db>.fsqlite-history` and `-idx`; `fsqlite-mvcc/src/history_sidecar.rs` | generation-governed history | Bind expected identity/recovery lineage through authority or refuse feature |
| Cold witness | sibling `.fsqlite/<stem>_witnesses.log`; `hot_witness_index.rs` | generation-governed correctness evidence | Authority resolver and transition rule or typed unsupported |
| Parallel-WAL certificate | `<db>-wal-cert`; `PathRefreshingWalBackend` | generation-governed durability | Admit/create under WAL authority; no independent path adoption |
| Certificate handoff | `<db>-wal-cert-head` | generation-governed temporary recovery | Authority-owned create/consume/cleanup with crash oracle |
| Parallel-WAL segments | `<db>-wal-seg-<epoch>`; `parallel_wal.rs` | generation-governed durability | Enumerate and bind complete segment set or refuse parallel-WAL mode |
| Namespace gate/use | `<db>-fsqlite-ns-gate`, `<db>-fsqlite-ns-use` | candidate generation-independent coordination | Prove format cannot be recovery data and intentionally survives A/B; securely resolve |
| Windows lock sidecars | VFS shared/reserved/pending paths | candidate generation-independent coordination | Enumerate exact suffixes and prove platform transition semantics |
| Publication candidates | VACUUM/validated-image temporary files | generation-governed publication | Strong connection refuses; weaker publication requires exclusive gate and effect-aware state |
| WAL-FEC temp | `.wal-fec.tmp` rewrite path | generation-governed temporary | Resolver ownership and crash-safe rename proof, or feature refusal |
| Memory VFS | process-local object identities | separate profile | Explicit process-local semantics; no native-path conformance claim |
| Custom VFS | caller implementation | unresolved | Typed unsupported until sealed owner adapter proves equivalent semantics |
| Unnamed temporary DB | no stable logical path | inapplicable | Typed refusal from the generation-bound constructor |

No omitted artifact is implicitly safe. Implementation discovery that finds an
additional correctness, repair, history, publication, lock, or temporary family
must update this packet and classify it before the feature can be enabled.

## 6. Main-handle lineage design

All authoritative main I/O receives an authority-derived lease, not a path.
The implementation must change or fence at least these observed escapes:

- `PathRefreshingWalBackend::conflicts_after_generation_change`;
- `SimplePager::export_database_bytes`;
- `SimplePager::copy_database_to`;
- header, schema, page-size, conflict, recovery, checkpoint, FEC, and repair
  helpers that open the main path;
- background and cancellation cleanup; and
- validated-image/VACUUM publication.

An observational path probe may check namespace state when the active profile
permits it. Its type must be incapable of promotion into authoritative I/O.
Handle-origin instrumentation must assign one unforgeable test lineage ID to the
admitted main and propagate it through capability-derived duplicates.

## 7. Publication gate

The current native namespace protocol already uses a shared lifetime `use` lock
and an exclusive generation transition. Implementation should evolve this one
protocol rather than create a competing ledger.

The exclusive publication route must cover:

- object replacement;
- validated/full-image installation;
- VACUUM publication;
- partial or whole repair/overwrite;
- truncate and extend outside pager coherence;
- compatibility, async, and C wrappers;
- pager-level helpers; and
- any future internal image installer.

For a live strong authority:

- publication through the strong connection returns a typed pre-effect
  `GenerationRotationUnsupported`-class refusal;
- a cooperative weaker route cannot acquire the exclusive gate and returns a
  typed busy/refusal without mutation; and
- ordinary authority-governed writes, admitted recovery, and checkpoint do not
  acquire that exclusive gate and remain enabled.

Before an allowed A-to-B transition publishes B, every generation-governed
artifact must be drained/retired, transferred by an owner-proved operation, or
cause pre-publication refusal. A stale A artifact must never remain admissible
as B.

## 8. Backend profiles

### Strong native local profile

Initial conformance should be limited to an explicit local-filesystem profile
that proves:

- stable live-handle identity;
- secure no-follow regular-file open;
- single-link enforcement;
- trustworthy cooperative advisory locking for the local filesystem;
- canonical artifact resolution; and
- typed refusal when filesystem classification is unknown.

The implementation plan must name the exact supported Unix filesystem set.
Remote, overlay, FUSE, clustered, reconnecting, and unrecognized mounts refuse.

### Windows

Full 128-bit handle identity, reparse behavior, file-share/delete semantics,
lock sidecars, SHM, parent/drive/UNC/case aliases, and weak legacy identity need
a separate proof profile. A weak fallback must not inherit strong conformance.

### io_uring

The Linux io_uring profile must prove real submission and completion. A test
that executes the Unix fallback proves only fallback. Each in-flight operation
retains an authority lease through completion or cancellation cleanup.

## 9. Error and effect model

The current `FrankenError` has `CannotOpen`, `Unsupported`, `Busy`, and recovery
variants but not the complete selected distinctions. Implementation must add a
typed contract that covers at least:

- unsupported backend/filesystem profile;
- identity unavailable or too weak;
- alias/link ambiguity;
- namespace generation drift;
- expected main identity mismatch before any database/recovery effect;
- sidecar provenance ambiguity;
- unsupported artifact family/feature combination;
- generation rotation unsupported;
- pre-effect admission refusal; and
- indeterminate database or recovery effect.

Exact Rust variant names are implementation choices. Each variant must map to
one of the three semantic outcome classes. Errors after a durable effect may
begin cannot be collapsed to `CannotOpen` merely because the final operation
failed.

## 10. Concurrency and lifetime

- `concurrent_mode_default` remains `true`.
- Admission locks may serialize admission/publication for one namespace, not
  transactions or all connections.
- MVCC writers continue to conflict at page granularity.
- The authority outlives pager caches, WAL/backend work, SHM users, background
  operations, and actual io_uring completions.
- Close/cancel cannot release namespace generation while dependent work lives.
- Proof must observe overlapping disjoint-page prepare critical sections from
  frozen mutation page-set entry through readiness for WAL/durability. Parsing,
  queue wait, transaction lifetime, and generic buffer staging do not qualify;
  throughput or successful stress alone is insufficient.

## 11. Conformance boundary

A proof matrix must enumerate every writable existing-open entrypoint. For each,
it records:

- whether it claims Decision-87 admission conformance;
- whether it can perform generation-changing publication;
- whether it participates in the namespace-wide publication gate;
- supported backend/features; and
- tests proving no silent fallback.

Only `Connection::open_existing_generation_bound` is conforming in the first
slice. The hidden pager factory is an unsupported composition seam, not a second
public contract.

## 12. Consumer boundary

Agent Kernel is not part of owner implementation proof. After owner proof on an
immutable FrankenSQLite revision, a separate consumer task may evaluate:

- exact dependency commit and feature set;
- exact public symbol and call graph;
- no weaker constructor fallback;
- authoritative execution and non-optional admitted-identity readback lineage;
- non-authorizing append-only durable receipt binding to the consumer-owned
  database identity, successful owner comparison, exact API/profile, owner
  revision, unique operation identity, and result digest without serializing
  the live expected/admitted value. A byte-identical duplicate for the same
  operation/result returns the original committed receipt without a second
  append or authority effect. Conflicting same-operation duplicates and all
  cross-operation or stale replay are rejected; a delayed exact-tuple retry is
  not stale, while a different or superseded operation/result tuple is stale;
- preserved ADR-0031 generation fencing; and
- explicit authorization before task 4195 resumes.

No owner document or green owner test authorizes pin rotation by itself.

## 13. Design completion gate

This packet is ready for implementation review only when reviewers can answer:

1. Does one private pager owner mint all authority?
2. Is `Exact(ExpectedMainIdentityGuard)` compared with retained-handle identity
   before generation-governed sidecar admission or every recovery/data effect,
   with only enumerated coordination effects and typed mismatch?
3. Does every success atomically return an authority-derived admitted identity
   that is not copied from caller input?
4. Is every observed artifact classified or refused?
5. Can no main-path probe become authoritative I/O?
6. Does every cooperative publication route share one exclusive gate?
7. Are ordinary writes/recovery/checkpoint still generation-preserving?
8. Are outcome classes impossible to collapse silently?
9. Is objective writer overlap required?
10. Are platform and io_uring claims separately proved?
11. Is downstream adoption still separately gated?
