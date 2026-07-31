---
summary: "Revised source, adversarial findings, and proof matrix for generation-stable database authority."
read_when:
  - "Evaluating evidence or validation obligations for FrankenSQLite issue 308 or Decision 87."
type: "evidence"
---

# Evidence: Generation-Stable, Sidecar-Proven Existing-Runtime Open

**Status:** Revised decision evidence after adversarial review
**Date:** 2026-07-31
**Owner issue:** [frankensqlite#308](https://github.com/Dicklesworthstone/frankensqlite/issues/308)
**Decision:** Agent Kernel decision 87
**Observed upstream revision:** `5676cb97486a62c4f0a19c053184e0ff3cfb2852`
**Observed consumer pin:** `c3c916e857ef55bed55a18b0c8b381bd9f2884e4`

## Evidence rules

This document distinguishes implemented behavior, source-observed gaps, and proposed proof. Source inspection is not runtime proof. Existing tests establish only the schedules and oracles they exercise.

Identity equality is not handle lineage. Canonical naming is not sidecar provenance. Stress success is not concurrent-writer overlap. A final successful check cannot erase earlier mutation.

## Upstream implementation evidence

| Concern | Observed source | What it establishes | Remaining gap |
| --- | --- | --- | --- |
| Open-handle identity | `crates/fsqlite-vfs/src/traits.rs`, `FileIdentity` and `VfsFile::file_identity` | Native and memory files can expose an opaque identity derived from the opened object | A connection-level authority owns the originating handle end to end |
| Expected-identity open | `crates/fsqlite-vfs/src/traits.rs`, `Vfs::open_with_expected_identity` | The object opened by a VFS can be compared with an expected identity | Equality does not prove derivation from the same admitted handle or prevent caller gaps |
| Namespace generation | `crates/fsqlite-vfs/src/namespace.rs`, `PendingNamespaceOpen` and `DatabaseNamespaceBinding` | Cooperative openers join or establish one recorded main identity | The record does not establish WAL/journal provenance or defeat raw namespace mutation |
| Pager admission | `crates/fsqlite-pager/src/pager.rs`, existing-only `ReadWriteOpenPolicy` path | Pager combines canonical path, namespace admission, expected identity, and recovery gates | Sidecar discovery/open and recovery effects are not one explicit admission transaction |
| Public checked open | `crates/fsqlite-core/src/connection.rs`, `Connection::open_existing_with_expected_identity` | A caller can request a writable open against a supplied identity | The caller still owns the preflight gap and namespace-stability obligation |
| Main retention | `crates/fsqlite-pager/src/pager.rs`, pager main-file storage | Normal pager I/O retains an opened main object | Other helpers still reopen the canonical path |
| WAL refresh | `crates/fsqlite-core/src/wal_adapter.rs`, `PathRefreshingWalBackend::conflicts_after_generation_change` | Conflict refresh can read current main pages | It reopens `db_path` as authoritative main I/O after construction |
| Export/copy | `crates/fsqlite-pager/src/pager.rs`, `SimplePager::export_database_bytes` and `copy_database_to` | Pager exposes database export/copy helpers | Both can reopen the source pathname instead of using retained authority |
| Main publication | `crates/fsqlite-pager/src/pager.rs`, `SimplePager::publish_validated_database_image`; caller in `crates/fsqlite-core/src/connection.rs` VACUUM flow | A live connection can publish a replacement database image | Retained generation authority has no defined rotation or refusal rule |
| Rollback recovery | `crates/fsqlite-pager/src/pager.rs`, journal path and recovery functions | Canonical rollback artifacts are found and recovered | Canonical location alone does not associate the journal with the admitted main generation |
| WAL installation | `crates/fsqlite-core/src/connection.rs`, WAL installers and `wal_path_for_db_path` | WAL uses the logical database path | Installation remains path-fed and provenance is not a first-class admission result |
| Unix WAL/SHM | `crates/fsqlite-vfs/src/unix.rs`, `sqlite_wal_path` and `sqlite_shm_path` | SQLite-compatible sibling names are derived | Secure alias handling and generation membership are not universally enforced |
| Windows sidecars | `crates/fsqlite-vfs/src/windows.rs`, SHM and lock-sidecar helpers | Windows-compatible sidecars are derived | Ordinary opens do not by themselves prove reparse refusal or generation membership |
| io_uring | `crates/fsqlite-vfs/src/uring.rs` | Current io_uring wraps and retains the Unix-opened main file | Tests that fall back to ordinary Unix I/O do not prove actual io_uring parity |
| Auxiliary artifacts | DB-FEC, WAL-FEC, MVCC history/witness, and parallel-WAL modules | Additional correctness artifacts exist outside ordinary WAL/journal paths | Their generation-authority classification is absent |
| Alias checks | `crates/fsqlite-vfs/src/lib.rs` and namespace transition validation | Some ingress and transition paths reject multiple hard links | The strong constructor does not yet make the rule universal across main and sidecars |

## Existing positive and refusal tests

Current tests prove useful pieces:

- matching expected-identity opens succeed;
- mismatched identity can refuse before selected recovery mutation;
- reserved identity opens refuse missing paths and selected recovery artifacts;
- namespace transitions can publish a replacement and reopen by new identity;
- Windows expected-identity paths have refusal/acceptance coverage; and
- io_uring wrappers can retain the Unix-opened file.

These tests do not prove issue #308. They do not collectively establish exact handle lineage, cooperative sidecar provenance, live-connection replacement semantics, complete artifact coverage, or objective writer overlap.

## Consumer evidence

Agent Kernel task 4195 reconciliation packet 82 v2 records the concrete downstream failure:

- one generation was identified;
- a later writable runtime open still occurred by pathname;
- exact A/B/A replacement could select B while receipts named A;
- an fd alias retained the main object but changed sidecar names; and
- no Agent Kernel wrapper could own both exact main authority and FrankenSQLite's artifact namespace.

Agent Kernel ADR-0031 disallows path-only fallback. Task 4195 remains held, and task 4196 remains transitively blocked.

## Adversarial review evidence

Decision 87's first review closure was superseded by a deeper adversarial pass:

| Review lane | Artifact | Controlling finding |
| --- | --- | --- |
| Architecture/threat model | `asc://dispatch-1785506353758` | Raw sidecar races and cooperative sidecar provenance were not resolved |
| Runtime/backend bypass | `asc://dispatch-1785506353761` | Live generation rotation and additional main/sidecar escape paths require architecture rules |
| Platform/VFS | `asc://dispatch-1785506353762` | Alias, parent-directory, filesystem, Windows, and typed-refusal requirements were under-specified |
| Proof/test oracle | `asc://dispatch-1785506353763` | The matrix could pass while lineage, effects, concurrency, or crash semantics remained wrong |
| API/governance | `asc://dispatch-1785506353764` | Public construction and weaker entrypoints could make conformance toothless |
| Adversarial synthesis | `asc://dispatch-1785506674823` | Outcome `revise_rfc`; retained-main direction survives, sidecar and transition contract does not |

Round-2 review of packet commit `fbf8f71cc7543259cdacc2b8868cb23dce4d19d9` produced two `ready_for_adr` lanes and three `revise_rfc` lanes. Controlling synthesis `asc://dispatch-1785507796599` preserved `revise_rfc` for four bounded corrections: namespace-wide publication exclusion, an explicit untagged-sidecar bootstrap rule, three-state effect semantics, and exclusive downstream result lineage.

## Falsifying schedules

### Raw sidecar substitution

1. Retain main handle A.
2. Validate the canonical main path as A.
3. Replace `db-wal` or `db-journal` with an unrelated artifact.
4. Allow the engine to open the canonical sidecar pathname.
5. Restore the original directory entry before a later check.

A retained main descriptor does not make step 4 atomic with step 2. This schedule is outside the cooperative guarantee unless a platform ownership primitive is separately proved.

### Cooperative stale sidecar

1. Generation A leaves a recovery-bearing artifact.
2. All A connections release their namespace bindings.
3. A cooperative transition publishes main B without retiring or admitting A's artifacts.
4. A B connection opens the canonical artifact.

All participants can obey the namespace protocol while provenance remains wrong. The transition protocol must close this schedule.

### Live publication split

1. A generation-bound connection retains main A.
2. The connection publishes validated replacement B at the canonical path.
3. Existing pager or backend state continues using A while later sidecar or helper paths observe B.

The first contract must refuse step 2 before effect or atomically rotate the entire generation authority.

### Untagged sidecar false provenance

1. Create main A and unrelated main B with compatible page size and database shape.
2. Leave a structurally valid recovery-bearing artifact from B at A's canonical sidecar name.
3. Admit it using canonical name, checksum, format, and replay compatibility alone.
4. Replay B's state into A.

The revised bootstrap rule requires an enumerated artifact-specific exact binding witness or pre-effect `SidecarProvenanceAmbiguous` refusal. Structural validity alone cannot pass.

### Weaker-peer publication bypass

1. A strong connection retains a generation authority for A.
2. A cooperative peer enters through a weaker constructor.
3. The peer attempts generation-changing publication: replacement of the main object or installation of an independently produced full logical image, including identity-preserving VACUUM/validated-image installation.
4. The strong connection resumes with stale caches or artifact state.

Every cooperative generation-changing publication entrypoint must acquire a namespace-wide exclusive gate that conflicts with the strong lifetime lease. Ordinary writes, authority-admitted recovery, and checkpoint remain generation-preserving I/O through the retained authority. Constructor conformance and publication exclusion are separate properties.

## Required proof matrix

| Proof | Required injection or observation | Pass condition |
| --- | --- | --- |
| Admission linearization | Deterministic hook at every admission stage | No database effect occurs before main, namespace, alias, and supported sidecars are admitted |
| Main-handle lineage | Tag every authoritative handle by origin | All main I/O descends from the admitted handle or an identity-preserving duplicate; no pathname-opened probe is promoted |
| Cooperative A/B/A | Attempt namespace-governed replacement throughout construction and connection lifetime | Transition is excluded or receives typed refusal while the authority is live |
| Cooperative stale sidecar | Leave each recovery-bearing artifact across a legal A-to-B transition | Transition retires/adopts it under the selected rule or refuses before publishing B |
| Raw main replacement | Replace A with B and restore A at deterministic hooks | Already-open authoritative main I/O stays on A; claims do not exceed checked-boundary detection |
| Raw sidecar replacement | Swap every supported sidecar between validation, existence check, open/create, and effect | Proved ownership profile prevents it, or the backend/threat mode is explicitly outside the strong guarantee |
| Sidecar bootstrap | Present absent, exactly bound, structurally valid but unrelated, and unbound recovery artifacts | Clean or explicitly bound artifacts admit; every unbound artifact returns pre-effect provenance refusal |
| WAL/SHM | Exercise non-empty WAL, SHM, append, checkpoint, restart, and recovery only where an exact binding rule is selected | Admitted/created artifacts remain in the cooperative generation domain; otherwise pre-existing recovery state refuses |
| Rollback journal | Exercise hot and non-hot journal inputs | Recovery runs only with an exact binding witness; canonical/format-valid ambiguity refuses before replay |
| Auxiliary artifacts | Enable WAL-FEC, DB-FEC, MVCC history/witness, and parallel-WAL separately | Each uses the authority or returns typed unsupported before effects |
| Post-return main reads | Force WAL refresh, export, copy, checkpoint support, and header/conflict reads | No path selects a new authoritative main handle |
| Generation-changing publication | Invoke object replacement and independently produced full-image VACUUM/validated-image installation through strong, weak, pager, compatibility, async, and C entrypoints; separately run ordinary writes, admitted recovery, and checkpoint | Strong publication refuses before effects; peer publication is excluded while authority is live; generation-preserving I/O remains enabled through retained authority |
| Alias handling | Exercise symlink, hard-link, reparse, mount/path alias, case, and relative-path inputs | Supported profiles prove secure admission; ambiguous inputs refuse |
| Parent replacement | Replace a parent between canonicalization and sidecar acquisition | No universal prevention claim; stronger profiles prove directory authority, otherwise threat limitation is explicit |
| Filesystem qualification | Run on local qualified filesystems and representative remote/virtual filesystems | Unknown or unproved filesystems refuse exact mode rather than silently weakening it |
| Windows probes | Trigger every identity-validation probe and fallback path | Probes never become authoritative; weak identity profiles refuse unless separately accepted |
| io_uring | Require actual submission and completion, with fallback measured separately | No main pathname reopen and in-flight requests retain authority until completion |
| Memory/custom VFS | Exercise supported and identity-less implementations | Process-local semantics are explicit; unsupported backends refuse |
| Temporary databases | Pass unnamed and named temporary inputs | Unnamed inputs are inapplicable; named support is claimed only with backend proof |
| Concurrent writers | Instrument overlapping disjoint-page write transactions and durability phases | Objective overlap exists; no global serialization or default-off MVCC fallback |
| Failure injection | Fail admission, sidecar open, bind, recovery, sync, validation, cleanup, and close | Outcome is pre-effect refusal, definite completion, or indeterminate effect; no class is mislabeled |
| Crash state | Crash at every recovery/publication cut and crash again during recovery | Recovery converges to a declared valid pre-operation or completed post-operation state and is idempotent |
| API conformance | Exercise every public writable existing-open entrypoint | Only the exact generation-bound symbol claims this contract; weaker APIs remain visibly non-conforming |
| Consumer return gate | Evaluate the exact dependency revision, features, and full result lineage in Agent Kernel | Every authoritative task-4195 execution, readback, and receipt descends exclusively from the conforming constructor with no weak fallback before explicit return authorization |

## Evidence still required before implementation acceptance

1. Artifact-specific exact-binding rules, or deterministic pre-effect refusal, for every pre-existing recovery family claimed by the first implementation.
2. A complete artifact classification for every feature reachable from the generation-bound constructor.
3. Concrete typed errors implementing the selected three-state effect semantics.
4. Qualified platform and filesystem profiles.
5. Deterministic handle-lineage, publication-gate, and sidecar-race instrumentation.
6. An exact immutable downstream dependency and exclusive result-lineage receipt.

These are proof requirements, not evidence that implementation already exists.
