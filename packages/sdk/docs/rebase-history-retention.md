# Retire remote rebase history without losing pending local edits

`ChangesetRebaseJournal.retireThrough(bookmark)` releases retained remote decision
entries and wire-byte capacity. The committed history position keeps advancing;
`maxEntries` now bounds retained decisions, not lifetime applications. No history
is deleted automatically. Original local edits use the separate `retireLocal`
method, and outgoing outbox identities retain their existing acknowledgement and
forgetting rules.

```ts
// Only after external consumers no longer need this prefix for replay/rebasing.
const boundary = await journal.bookmark();
const result = await journal.retireThrough(boundary);
// Snapshot-backed databases still require a genuine same-database checkpoint.
await source.checkpoint();
console.log(result.removed, result.byteLength, result.retainedEntries);
```

The boundary must be a complete, exact bookmark for this journal, not just a
numeric position. Its hash is verified against the existing history before any
write. The result includes `floor`, the retained bookmark at the end of the removed
prefix, plus decisions and wire bytes removed in this call. This is not a disk
space/VACUUM result. `journal.retention()` reads the current floor, retained entry
count and retained decision bytes without removing anything. It remains available
under reduced aggregate capacity settings.

## What remains recoverable

A versioned, checksummed checkpoint in the optional reserved
`__fsqlite_rebase_journal_retention` table retains the original complete-prefix
hash. The suffix continues the SAME bookmark chain, rather than starting a new
journal or inventing new history. An unchanged tip therefore has the identical
bookmark before and after retirement. `bookmark()`, later `captureLocal()`, and
rebasing from a retained basis all continue from that checkpoint.

Every retained original pins its saved basis, including net-zero or already
published originals. Retirement refuses to cross any such basis. It verifies
original record and payload checksums while examining pins, so changing a basis
counter alone cannot hide a dependency. The boundary itself is allowed: an
original captured at position N needs decisions AFTER N, and its saved bookmark
can still be verified from the retained floor. Published originals can be removed
with `retireLocal` only after their required deliveries are acknowledged; a slow
required replica therefore prevents premature loss of the associated history.

A rebasing request with an `after` or `through` below the floor rejects with
`ERR_FSQLITE_REBASE_JOURNAL_EXPIRED`. The default `rebase(bytes)` basis is still
zero: it is NOT silently advanced. Supply the original retained basis or use
`rebaseLocal`. Retrying retirement at the exact current floor removes nothing
but still verifies the requested bookmark and live suffix. An older, already
retired boundary rejects as expired; use `retention()` to reconcile current state.

Positions now accept nonnegative safe integers through Number.MAX_SAFE_INTEGER;
retained entry, message and byte caps are unchanged. A saturated position refuses
new application instead of overflowing. Head `byteLength` describes retained
remote-decision bytes and decreases on explicit retirement.

## Replay and application responsibilities

**Inbox receipts are never deleted by this operation.** A previously applied
message therefore cannot run its application SQL again. However, journaled replay
requires the original decision entry; after that entry has been retired it rejects
rather than fabricating rebase information or a successful journaled receipt.
`read(deliveryId)` returns null for a no-longer-retained decision, as for other
missing entries. Do not interpret that null or a replay error as permission to
repeat application under another delivery identity.

Retire only after external replay and rebasing consumers no longer need the
prefix. The method cannot discover original changesets held outside this journal,
prove remote acknowledgement or confirm a retry horizon. In particular, retained
incoming receipt metadata is not independent proof of remote storage. Never
recycle delivery IDs or manually remove inbox evidence to bypass replay checks.
Inbox storage, outgoing identities, originals and decision history have distinct
retention obligations; this does not bound the entire database.

## Atomicity, integrity and compatibility

Checkpoint publication, prefix deletion and byte-accounting updates use one owned
SQL transaction. The complete current history is verified before removal, and the
surviving suffix must recover the same tip hash afterward. Exact row populations,
contiguous positions, message/decision hashes, application receipts, checkpoint
layout and seal are checked. Missing checkpoints, missing live entries, changed
heads, unexpected indexes/triggers, and partial or orphan storage fail closed.
Nothing recreates missing history. Local database/schema ownership remains trusted;
checksums are not authentication against a writer who can forge all metadata.
Whole-database rollback restores its old checkpoint too; no external anti-rollback
service is provided.

Cancellation and deadlines wait for admitted SQL to settle. SQL errors, false
write counts, failed COMMIT and enclosing-transaction rollback undo the complete
retirement. After a lost COMMIT response, reopen and reconcile the same bookmark;
committed retirement is not labeled a rollback. Independent connections rely on
the owner's normal snapshot/conflict handling; no global writer lock, automatic
retry, transport operation, or background cleanup is added.

Existing databases need no migration until the first nonempty retirement. Older
SDK versions do not understand the resulting gaps and cannot service the compacted
history; upgrade journal writers/readers together. No compatibility fallback
renumbers positions or silently restores removed decisions. The new methods are
available through the already exported ChangesetRebaseJournal class.

Verification retains one decision payload at a time, plus bounded original-key
pages of at most 32 and one original payload while checking pins. Total work is
linear in the retained history and original population, not constant time.
Original/output/codec buffers can coexist. These bounds are logical data budgets,
not total engine memory, process RSS, or latency guarantees.

## Executed verification

```sh
node --experimental-transform-types \
  --experimental-loader=./packages/sdk/tests/helpers/production-source-loader.mjs \
  --test packages/sdk/tests/changeset-journal-retention.test.mjs
```

The targeted suite executes production journal, capture, rebase, application,
codec, foreign-key, outbox-store and fanout modules with no backend substitutes.
Node 22.16.0 / SQLite 3.49.1 supplies SQL ownership. Native Session changesets and
native application verify rebasing convergence after pruning in UTF-8, UTF-16LE
and UTF-16BE. Tests cover capacity reuse, unchanged bookmarks, retained local pins,
slow replicas, corruption, lowered caps, escaped identities, indexed keyset bounds,
outer/deferred rollback, cancellation drain, lost replies and independent WAL
readers/writers. Synthetic high-position fixtures test numeric boundaries only;
they are not evidence that those enormous histories were executed.

Eight child processes send a requested cut marker and are actually SIGKILLed after
checkpoint publication, deletion, and before/after COMMIT under WAL/DELETE journals.
A watchdog kill fails the test. Fresh owners recover all-or-none state and continue
applying without resetting positions. Strict TypeScript checking covers all eight
actual transitive source modules without declaration substitutes. This is not a
rerun of every prior SDK suite or qualification of full SDK/worker packaging,
FrankenSQLite Rust/WASM/MVCC, HTTP/TLS, browser storage or physical power loss.
