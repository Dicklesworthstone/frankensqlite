# Persistent changeset rebase history

`ChangesetRebaseJournal` couples received changesets, their conflict decisions,
and the existing delivery inbox in one owned SQL transaction. It removes the
need to implement a custom `onRebase` journal to survive restart or a lost
acknowledgement. It is a SQL-backed SDK feature, not native ECS replication or a
new durability mode.

```ts
import { ChangesetRebaseJournal } from '@frankensqlite/sdk';

const history = new ChangesetRebaseJournal(db, {
  journalId: 'device-42:local-history',
  maxEntries: 10_000,
  maxBytes: 64 * 1024 * 1024,
});
const applied = await history.apply(remoteBytes, {
  deliveryId: 'trusted-peer-7:operation-108',
  tables: ['notes'],
  // Conflict policy belongs to the application. Default is abort.
  onConflict: conflict => conflict.kind === 'data' ? 'omit' : 'abort',
});
console.log(applied.entry.position, applied.replayed);

// After reopening the SAME database with the SAME journal identity:
const entry = await history.read('trusted-peer-7:operation-108');
const result = await history.rebase(originalLocalChangeset, {
  after: localHistoryBasis,
  through: applied.entry.position,
});
// result.changeset is new, owned output. It does not alter existing outbox bytes.
```

## Transaction and identity boundaries

The journal uses the existing `applyChangeset` schema checks, explicit table
allowlist, conflict policy, cancellation checks, and receipt machinery. Its
`onRebase` hook is internal. Callers cannot replace it or override the journal's
configured codec limits for one application. Ordinary table triggers and
foreign-key effects still run; the allowlist is not a security sandbox.

A fresh application appends exactly one journal entry, including applications
with no conflicts (an empty BLOB). Entries are numbered consecutively within
one journal identity. The rows, proof, position/byte counters, and inbox receipt
commit or roll back together. A full journal rejects and rolls back the entire
new application; it does not silently throw away old decisions.

An exact delivery replay returns the original entry and counts, without running
the SQL or conflict callback again. A pre-existing receipt without a matching
journal entry is an explicit `ERR_FSQLITE_REBASE_JOURNAL_MISSING` error. Decisions
from an older unjournaled application cannot be reconstructed by rerunning its
policy against today's rows. Delivery IDs remain globally source-qualified and
case-sensitive; they cannot be moved between journal identities.

`head()` returns the current position and retained wire-byte count. `read(id)`
returns an owned decision buffer or null for an unknown entry. Reading an empty
journal does not create its storage. Reopening with the same identity recovers
history from actual SQL tables, not a retained JavaScript object.

The target must provide real transaction ownership. A `FrankenTransaction`
target uses a child scope and remains provisional until its enclosing commit.
Snapshot persistence still requires an explicit successful checkpoint of the
same database before externally acknowledging durability. A memory database is
not durable merely because it has journal tables. A lost commit response is
uncertain: reopen/reconcile and retry the same ID and bytes, never a new ID.

## Receiving deliveries with persistent history

`ChangesetReceiver` accepts an optional `rebaseJournal` configuration. It creates
the journal on its own SQL target, not a separately supplied database, and runs
received changes through that journal before the existing confirmation barrier:

```ts
import { ChangesetReceiver } from '@frankensqlite/sdk';

const receiver = new ChangesetReceiver(db, {
  receiverId: 'device-42',
  tables: ['notes'],
  rebaseJournal: { journalId: 'device-42:local-history', maxEntries: 10_000 },
  // Application-owned checkpoint/recovery barrier for this same database.
  confirmCommit: confirmDatabaseCommit,
  onConflict: conflict => conflict.kind === 'data' ? 'omit' : 'abort',
});
const receipt = await receiver.receive(authenticatedEnvelope);
const history = receiver.rebaseJournal!;
const savedDecision = await history.read(receipt.deliveryId);
const rebased = await history.rebase(originalLocalChangeset, { after: localHistoryBasis });
```

The receipt and delivery protocol are unchanged: no journal payload, position,
or local policy is added to the wire. The accessor returns local history, or
`null` when the feature is not configured. Configure it in trusted application
code; fields in an incoming message cannot enable or replace it. Coordinate
direct accessor use with the same database ownership and lifecycle as other SQL.

A journaled SQL commit does not by itself produce a receiver ACK. Confirmation
must finish, including on an exact replay. If SQL commits but confirmation or
response delivery fails, the original journal entry remains available; retry the
same envelope after reconciliation instead of rerunning SQL or selecting a new
identity. Cancellation after commit can withhold the receiver ACK without undoing
committed history. The existing receiver still refuses concurrent receives rather
than queuing unbounded work. A full/corrupt journal or an unjournaled prior receipt
prevents confirmation; it never silently falls back to the unjournaled path.

Reopen with the same local `journalId`. Turning journaling on for previously
unjournaled deliveries cannot reconstruct their decisions; those receipt replays
fail explicitly. Receivers without this option retain their previous application
path and do not create journal tables. Existing transports and pumps keep their
unchanged confirmation/receipt rules; this does not turn local history into an
authenticated remote rebase protocol or automatically rewrite queued payloads.

## Ordered rebasing

`rebase(originalLocalBytes, { after, through })` reads one SQL snapshot and
configures the existing `ChangesetRebaser` in stored application order. It
excludes `after` and includes `through`; the defaults are zero and that
transaction's journal tip. An absent requested range is an error, never a
fallback to current data. The returned bounds identify the history used.

The caller owns the local changeset's basis: it must be the original local
changes against which those remote decisions were made. Do not repeatedly
feed already-rebased output through the same history, combine unrelated local
histories under one identity, or choose bounds from another database. Store a
local operation's basis with its original changes using the same transaction
ownership. Applying remote changes through paths that bypass this journal does
not record their decisions and is not repaired automatically.

Rebasing does not mutate database rows, mark a delivery acknowledged, transmit
anything, change an already-identified outbox payload, or implement a complete
bidirectional synchronization protocol. Patchsets are rejected because they
lack the before-images required for this contract.

## History-bound bookmarks

Use `await history.bookmark()` to capture the current prefix as a frozen,
JSON-serializable `RebaseJournalBookmark`. Store it with the original local
changeset under the same transaction ownership. Numeric positions alone cannot
detect a restored backup that has reused positions on a different history.

Both range endpoints accept a bookmark instead of a number:

```ts
// originalBasis was captured with the ORIGINAL local changes, before these
// remote decisions. Persist both, not just originalBasis.position.
const result = await history.rebase(originalLocalChangeset, {
  after: originalBasis,
  through: savedRemoteTip, // Optional bookmark; omit to use this SQL snapshot's tip.
});
console.log(result.afterBookmark, result.throughBookmark);
```

The returned bookmarks belong to the same SQL snapshot as the rebasing; they
are not obtained from a separate later head read. `afterBookmark` identifies
the excluded basis, and `throughBookmark` identifies the included prefix.
Persist the original basis for future rebasing of the original bytes. A returned
tip is not permission to feed already-rebased output through history again.

Each bookmark contains `format: 'fsqlite-rebase-bookmark-v1'`, `journalId`,
`position`, and a lowercase SHA-256 digest. The digest chains the entire prefix,
not just the last delivery: it binds the previous hash, position, delivery ID,
message digest/length, and verified decision digest/length. Empty decisions
also advance the chain. A matching final entry cannot conceal an earlier fork.
The chain uses UTF-8 JSON arrays with the versioned format as its domain; its
genesis binds that domain and the journal identity. No schema migration or
stored hash column is needed, so existing journal rows can be bookmarked.

A present position with the wrong prefix (or another journal identity) fails
with `ERR_FSQLITE_REBASE_JOURNAL_HISTORY`. An unavailable range still fails with
`ERR_FSQLITE_REBASE_JOURNAL_MISSING`. Neither path changes rows, repairs history,
replays remote SQL, or silently selects another basis. Exact copied/restored
prefixes remain valid; the bookmark is a logical history identity, not a unique
physical-database ID. Empty journals with the same ID share the same genesis.
Hashes are not authentication, proof of unchanged application tables, a rollback
oracle without a saved bookmark, or protection if a writer replaces the saved
bookmark too. Checkpoint/outer-commit requirements remain unchanged.

Bookmarking and rebasing verify entries from position one through the selected
tip, including entries excluded by `after`; corruption in that prefix is not
ignored. Bodies are verified against their checksums and inbox receipts one at
a time, with existing entry/byte limits and cancellation/deadline checks.
Work is linear in the prefix length; no constant-time or RSS guarantee is made.
Numeric endpoints remain an explicit position-only selection and cannot detect
a coherent alternative history without a saved bookmark.

## Durable original local changes

`captureLocal(operationId, work, options)` closes the gap between applying local
SQL and preserving the original changeset with its exact remote-history basis.
It runs the existing SQL-trigger capture path, verifies the history bookmark,
and saves the original bytes in the **same owned transaction** as the callback's
application writes. No separately timed bookmark read or application-managed
byte journal is required:

```ts
const captured = await history.captureLocal(
  'device-42:local-operation-109', // Permanently identifies this same work.
  async tx => {
    await tx.execute('UPDATE notes SET body = ? WHERE id = ?', ['local edit', 7n]);
    return { edited: 7n };
  },
  { tables: ['notes'], timeoutMs: 10_000 },
);

// After reopening this same database and journal:
const original = await history.readLocal('device-42:local-operation-109');
const outgoing = await history.rebaseLocal('device-42:local-operation-109', {
  through: savedRemoteTip, // Optional number/bookmark; omit for this snapshot's tip.
});
// outgoing.changeset is derived output; original.changeset remains unchanged.
```

Capture inherits `captureChangeset`'s explicit table allowlist and limitations:
ordinary main tables with declared primary keys, `recursive_triggers=ON`, no
application triggers on captured tables, and no callback schema changes. Enable
that PRAGMA before entering capture, not inside its callback. The callback must
await its SQL operations, and its external effects are not transactional. The
allowlist is not a sandbox. Do not mix remote journal application into a local
capture callback: changed history causes the entire capture to roll back.

A fresh result has `replayed: false`, the callback's `value`, and a `record`.
The saved `RebaseJournalLocalRecord` contains the operation ID, original bytes,
verified `basis` bookmark, payload `sha256`, `recordSha256`, byte length, change
count, and touched-row count. Metadata and the basis are frozen; every read owns
fresh mutable byte storage. The record checksum binds the payload digest, basis,
identity, capture scope, and counters. These hashes detect corruption; they do
not authenticate a writer or authorize local SQL.

An existing exact operation ID returns `replayed: true` and the original record
without calling `work`, recapturing today's rows, or advancing its saved basis.
Callback values are not persisted and are absent on replay. The capture scope
(table set and indirect flag) must match the original; table case/order alone
may differ. The ID is the application's assertion of the same work, not a hash
of the callback. Never reuse it for another edit. Empty/net-zero operations
retain records too, so their callbacks are not repeated after a lost response.

`readLocal(id)` returns null for unknown IDs without creating storage. It
verifies bounded metadata, payload, counts, and the bound basis checksum, but
does not require the remote history still to exist. This allows recovery of the
original bytes for explicit reconciliation even after a history restore or loss.
Deleting a record, losing the entire local table, or restoring a backup from
before capture removes this deduplication evidence. Absence is not proof that an
operation never committed elsewhere or before that restore. No automatic replay
or retry policy is supplied.

`rebaseLocal(id)` loads that saved original and verifies its original basis and
selected remote prefix in **one SQL snapshot**. It does not take replacement
bytes or an `after` override. Every invocation starts from the retained original,
never the output of the previous rebasing. It shares the existing `rebase`
algorithm, range checks, full-prefix bookmarks, cancellation, and deadline
checks. Missing originals/history fail explicitly; a coherent fork with the
same numeric basis fails the history check. Both returned bookmarks identify
that same snapshot, not a separate later head read. Neither original bytes nor
application rows are modified by rebasing.

Original retention uses the optional reserved
`__fsqlite_rebase_journal_locals` table, created only for local capture. Its
WITHOUT ROWID/BINARY primary key, columns, and lack of metadata triggers or
foreign keys are validated using the existing journal checks. Existing remote
journal schemas are unchanged. `maxLocalEntries` defaults to 10,000 (maximum
100,000), including empty operations; `maxLocalBytes` defaults to 64 MiB
(maximum 1 GiB) of retained original wire bytes. These budgets are independent
of remote decision retention. Full entry capacity refuses work before the
callback, and an over-byte-budget capture rolls back the callback's writes.
Per-message codec limits still apply. Budgets are not database-file or RSS caps.

Original capture verifies the complete remote prefix before and after work;
that cost is linear in retained history. There is no automatic expiry, pruning,
local queue enumeration, outgoing delivery-ID assignment, payload rewriting,
sending, or acknowledgement. In particular, never replace bytes already bound
to an outbox delivery identity with new rebased output. Multiple local operations
still require an application-owned synchronization and ordering protocol; these
APIs alone do not implement complete bidirectional synchronization.

Transaction and durability boundaries remain the target's own: a nested target
is provisional until its outer commit; snapshot-backed SQL still needs an
explicit successful checkpoint of the same database. If acknowledgement is lost,
reopen/reconcile the authoritative database and look up the same operation ID.
Do not infer rollback from an error after commit or choose a fresh operation ID.

## Bounds and integrity

`maxEntries` defaults to 10,000 and has a hard maximum of 100,000.
`maxBytes` defaults to 64 MiB and has a hard maximum of 1 GiB; it accounts for
retained rebase wire bytes, not database page overhead or total memory.
`limits` configures the existing per-message codec and combined in-memory
rebaser limits (including its independent default 64 MiB bound). A journal
can fit on disk while its chosen combined rebase exceeds a configured memory
accounting limit; that operation fails without changing the stored history.

Rebase reads fetch one bounded decision body at a time rather than returning
all history blobs. Scratch memory still includes input, decoded records, the
combined rebaser, SQL-engine allocations, and output; no heap/RSS ceiling is
claimed. The retained-entry bound also counts zero-byte decisions. There is no
pruning/expiry API: removing history would require an explicit local-changeset
retention frontier and a safe acknowledgement protocol.

Storage uses reserved `__fsqlite_rebase_journal_heads` and
`__fsqlite_rebase_journal_entries` tables. Admission validates their column/key
layouts, BINARY identity semantics, and absence of foreign keys or main/TEMP
triggers. Missing half-schemas, gaps, malformed counts/lengths, checksum failures,
and mismatches with the associated inbox receipt fail closed. Hashes bind
records to bytes, not to an authenticated sender. The local schema, SQL adapter,
and database writers must be trusted; this is not protection against an actor
who can coherently rewrite both journal and inbox. Authenticate remote input.

## Verification

```sh
node --experimental-loader=./packages/sdk/tests/helpers/source-loader.mjs \
  --test packages/sdk/tests/changeset-rebase.test.mjs \
  packages/sdk/tests/changeset-rebase-bookmark.test.mjs \
  packages/sdk/tests/changeset-local-capture.test.mjs
```

The tests execute production TypeScript journal SQL against Node SQLite and
compare decisions/rebased records with the actual native C SQLite
session/apply_v2/rebaser APIs. They cover persistent reopen, lost ACK, exact
replay, scoped positions, empty decisions, entry/byte bounds, checksum/accounting
corruption, missing receipts, main/TEMP triggers, cancellation around both writes,
I/O exceptions, deferred COMMIT failure and enclosing-transaction rollback.
Receiver integration tests also execute the actual `ChangesetReceiver` with the
production journal: suspended/failed confirmation, busy admission, lost SQL ACK
and file reopen, exact wire receipts, local configuration capture, unchanged
unjournaled default, backpressure, corrupt/missing history, ordered native-oracle
rebasing, and cancellation before or after committed history.
They are not certification of the FrankenSQLite Rust/WASM engine, browser
storage, native ECS, or power-loss behavior.

The bookmark suite uses actual Node SQLite files to fork a backup, reuse
positions with an identical final entry, and reject the saved conflicting
prefix without modifying storage. It also checks exact reopen, empty decisions,
message/policy identity, all valid small ranges, excluded-prefix corruption,
untrusted bookmark fields, input ownership, cancellation, deadlines, provisional
outer rollback, and same-snapshot result identities.

The local-capture suite runs the production SQL-trigger capture, journal, and
stored-original rebaser over real Node SQLite. Native session comparisons check
captured changes, and native changeset application checks OMIT/REPLACE rebasing
outcomes. It covers lost ACK plus reopen, actual child-process exits immediately
before/after COMMIT, rollback, deferred FK commit errors, cancellation, deadlines,
retention limits, malformed metadata, composite keys/scalars, same-snapshot
reads, and a valid saved original transplanted onto a coherent history fork.
Process-exit recovery is not simulated power-loss or browser durability proof.
