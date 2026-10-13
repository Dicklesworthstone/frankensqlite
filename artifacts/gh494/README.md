# GH494 explicit-COMMIT integration candidate — UNAPPLIED

`explicit-commit-integration.patch` preserves the Connection implementation
candidate from the preceding session. **This directory is an artifact, not
compiled engine source. Nothing applies this patch during a build. Publishing
it does not fix GH494, change SQL COMMIT behavior, or establish a speedup.**

The retained patch is byte-for-byte identical to the uploaded draft:

- Git blob: `35b60c2593355d17ed11e8bac4dab4cca55a444a`
- SHA-256: `5040d1f3e6f0322b9e63ac4d19486350988924666af59cacc0adea87193bfde7`
- Size: 20,264 bytes.
- Connection source basis: blob `674d8474074ab85711ce35ea70b5f3fe4aa1a083`.

Current main has advanced since that source basis. Do not replace current
Connection source with an old copy. Hunk offsets are approximate relocation
hints from inspected source sections; application to a complete checkout and
review of all changed call sites remain outstanding.

## Intended implementation

The diff collects `pending_commit_pages()` after deferred-write, foreign-key
and virtual-table synchronization, before the original explicit pager commit
consumes its mutation surface. It retains Connection's existing MVCC validation,
busy retries, settlement, rollback and attached-participant commit ownership.

TimeTravelImage uses PersistentPageMap rather than a flat page vector. Capture
pins a reader, checks physical sequence and page-size continuity, and updates
only pending pages, page one and newly allocated addresses. A missing or stale
predecessor is fully seeded; loss of the exact commit boundary rejects capture
instead of publishing a mislabeled image. History-capture failure follows the
existing log-and-skip path, not a retryable failure of a committed SQL write.

Four drafted unit tests exercise capture-read scaling through 32 MiB of
unrelated rows, historical trigger/index visibility, savepoint/full rollback,
and reseeding after a disabled-capture gap. The large case performs 101 point
updates per resident-data stage. These tests are inside the patch and are NOT
compiled or run merely by committing this artifact.

## Validation and remaining work

On 2026-10-13, `git apply --numstat` parsed the retained diff: 196 additions /
18 deletions in Connection and 184 added test lines. Added-line whitespace
checks passed, and the embedded test file matches the separate draft attachment.
The uploaded Git blob matches the local reviewed patch. These checks are NOT
`git apply --check` against current source, Rust compilation, or runtime tests.
A Cargo test attempt could not start (`cargo` absent, exit 127). No native
profile or before/after performance measurement ran in this session.

The candidate does NOT remove whole-MemDatabase hydration on subsequent reads
and does NOT change retained-autocommit capture. Completing GH494 requires
those production paths, full-source integration, and target-engine validation.
Keep `gh494_memory_commit_scaling` and
`bd_zjocc_time_travel_history_semantics` unchanged; do not weaken their gates,
disable history, or omit the transaction-plus-first-read envelope to claim a
win. The existing pager snapshot-owner tests must also run. Workspace check,
Clippy, rustfmt and actual original-workload profiling remain required.
