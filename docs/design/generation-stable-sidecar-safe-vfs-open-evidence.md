---
summary: "Source and proof matrix for the generation-stable, sidecar-safe open decision."
read_when:
  - "Evaluating evidence or validation obligations for FrankenSQLite issue 308."
type: "evidence"
---

# Evidence: Generation-Stable, Sidecar-Safe Existing-Runtime Open

**Status:** Decision evidence
**Date:** 2026-07-31
**Owner issue:** [frankensqlite#308](https://github.com/Dicklesworthstone/frankensqlite/issues/308)
**Observed upstream revision:** `5676cb97486a62c4f0a19c053184e0ff3cfb2852`
**Observed consumer pin:** `c3c916e857ef55bed55a18b0c8b381bd9f2884e4`

## Evidence rules

This document distinguishes implemented behavior, consumer evidence, and proposed proof. Source inspection is not runtime proof. Existing tests establish only the conditions they actually exercise.

## Upstream implementation evidence

| Concern | Observed source | What it establishes | What it does not establish |
| --- | --- | --- | --- |
| Open-handle identity | `crates/fsqlite-vfs/src/traits.rs`, `FileIdentity` and `VfsFile::file_identity` | Native and memory VFS files can expose an opaque identity derived from the opened object | A public connection constructor owns the originating main-file capability end to end |
| Identity-checked VFS open | `crates/fsqlite-vfs/src/traits.rs`, `Vfs::open_with_expected_identity` | The file actually opened by a VFS can be compared with an expected identity | The expected identity and runtime file necessarily originate from one owner-issued capability without caller gaps |
| Namespace generation | `crates/fsqlite-vfs/src/namespace.rs`, `PendingNamespaceOpen` and `DatabaseNamespaceBinding` | Cooperative openers join or establish one recorded generation and retain a lifetime binding | Arbitrary non-cooperating pathname replacement is prevented by advisory namespace records alone |
| Pager admission | `crates/fsqlite-pager/src/pager.rs`, existing-only `ReadWriteOpenPolicy` path | Pager combines canonical full pathname, namespace admission, expected identity, main-file identity, and recovery gates | A public consumer cannot introduce a gap before pager admission; all future sidecar opens are capability-bound rather than merely pathname-derived |
| Public checked open | `crates/fsqlite-core/src/connection.rs`, `Connection::open_existing_with_expected_identity` | Callers can request an existing-only writable open tied to a supplied identity | The API owns the preflight descriptor or removes its documented caller obligation to prevent replacement during open/recovery |
| Canonical WAL path | `crates/fsqlite-core/src/connection.rs`, `wal_path_for_db_path` and WAL installers | WAL remains a sibling of the logical database path | WAL and main cannot belong to different generations under an unclosed replacement window |
| Canonical journal path | `crates/fsqlite-pager/src/pager.rs`, `journal_path` and open-time recovery | Rollback journal remains a sibling of the logical database path | Journal recovery is bound to the same generation as a separately preflighted consumer identity under every race |
| io_uring handle reuse | `crates/fsqlite-vfs/src/uring.rs`, `IoUringVfs::open*` | Current upstream wraps the Unix-opened file and forwards its identity | Future changes or older consumer pins cannot regress to a split pathname open without dedicated contract tests |
| Post-construction WAL conflict read | `crates/fsqlite-core/src/wal_adapter.rs`, `PathRefreshingWalBackend::conflicts_after_generation_change` | Current upstream can reopen `db_path` as a new authoritative main handle after a WAL-generation change | The constructor-only capability is sufficient unless this path is routed through the retained generation |
| Windows identity probe | `crates/fsqlite-vfs/src/namespace.rs`, `DatabaseNamespaceBinding::validate_path_identity` | Windows may need a temporary handle to query its robust file identity | The probe is guaranteed never to become an authoritative pager/backend handle without an explicit contract test |

## Existing positive and refusal tests

Current upstream facade tests prove useful pieces:

- expected-identity open accepts a matching generation;
- mismatched expected identity refuses before hot-journal recovery mutation;
- reserved identity open refuses missing paths and pre-existing recovery artifacts;
- namespace generation transition can publish a replacement and reopen it by its new identity;
- Windows expected-identity paths have explicit refusal/acceptance coverage.

These tests do not constitute the required issue-308 proof unless they inject exact replacement at each constructor boundary and prove canonical sidecar behavior for the connection that is returned.

## Consumer evidence

Agent Kernel task 4195 reconciliation packet 82 v2 records the concrete failure mode:

- the prior candidate identified one generation;
- a later writable runtime open still occurred by pathname;
- exact A/B/A replacement could select B while receipts and generation readback named A;
- a held-descriptor alias avoided that main-file reopen but changed sidecar names;
- no AK-only wrapper could simultaneously own exact main identity and FrankenSQLite's canonical sidecar semantics.

Agent Kernel ADR-0031 requires generation-fenced authoritative readback and disallows path-only fallback. Task 4195 therefore remains held; task 4196 remains transitively blocked.

## Related issue evidence

- **#140:** read-only/schema-only opens must avoid mutation while preserving ABA safety and coherent locking.
- **#141:** `VACUUM INTO` needs source/candidate receipt CAS under concurrent writers.
- **#307:** macOS lacks the bounded external snapshot ownership primitive used on Linux/Windows.

They demonstrate broader portfolio pressure for owner-level generation contracts. They are not substitutes for issue #308's writable constructor semantics.

## Required proof matrix

| Proof | Required injection or observation | Pass condition |
| --- | --- | --- |
| Cooperative main generation | Perform namespace-governed A/B/A transition attempts during construction and connection lifetime | Binding excludes the transition or constructor refuses before effect |
| Raw-filesystem main generation | Replace A with B and restore A at deterministic hooks without honoring namespace locks | Already-open authoritative handles stay on their generation or the operation refuses; no claim that advisory locks prevent every rename |
| Post-construction WAL refresh | Force a WAL-generation change, then inject A/B/A while conflict validation reads main pages | Conflict validation uses the retained generation and never promotes a pathname-reopened main handle |
| WAL namespace | Exercise non-empty WAL, open/recovery, append, and checkpoint | Main, `-wal`, and `-shm` are one admitted namespace; no `/proc/self/fd` sidecars or mixed generation |
| Rollback namespace | Exercise hot/non-hot journal and recovery | Main and `-journal` are one admitted namespace; mismatch refuses before recovery mutation |
| io_uring | Run the same replacement schedule with io_uring enabled | No second pathname-selected main generation; identity matches the retained Unix handle |
| Windows | Replace the stable path between identity capture and checked open | Typed refusal or correct binding before lock-sidecar/recovery mutation |
| Windows probe authority | Trigger every handle-based identity validation probe | Probe handles are never adopted by pager, WAL backend, or recovery as authoritative main I/O |
| Temporary databases | Pass unnamed temporary and named temporary inputs | Unnamed inputs return typed inapplicable/unsupported; named support is claimed only with backend proof |
| Memory/custom VFS | Exercise implementations with and without stable identities | Supported backends implement the contract; unsupported backends return an explicit typed refusal |
| Concurrent writers | Run instrumented overlapping multi-writer transactions, checkpoint, recovery, and crash schedules | Objective overlap witness is present, MVCC concurrency remains enabled, and no global serialized fallback occurs |
| Failure injection | Fail open, identity query, namespace bind, sidecar open, recovery, final validation, and cleanup | No false success, leaked generation lease, or partial namespace publication |
| Consumer return gate | Evaluate the exact proposed FrankenSQLite revision in Agent Kernel | Task 4195 removes the unsafe reopen, passes A/B/A and sidecar tests, and records an explicit return authorization |

## Unresolved facts for the RFC

1. Whether the public contract should expose an opaque opened-generation capability or keep it entirely internal to `Connection`/pager.
2. Which platforms can strengthen the explicitly limited non-cooperating same-UID boundary beyond retained-handle safety and typed refusal.
3. Whether every supported backend can retain canonical sidecar semantics or some must refuse.
4. Whether sidecar binding is established eagerly during construction or through one capability-aware resolver retained by the connection.
5. Which current upstream primitives can be adopted without widening or duplicating namespace authority.

These are decision inputs, not implementation assumptions.
