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
  --test packages/sdk/tests/changeset-rebase.test.mjs
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
