# Seeding existing rows as a changeset

`snapshotChangeset` reads selected application tables at one SQL snapshot and
encodes their existing rows as an INSERT-only SQLite session changeset. It
complements callback mutation capture: a new replica needs a baseline, not only
changes made after capture began. This is logical row transfer, not a native
RaptorQ snapshot, schema migration, database-file backup, or replication protocol.

```ts
import { snapshotChangeset, applyChangeset } from '@frankensqlite/sdk';

const snapshot = await snapshotChangeset(source, {
  tables: ['parents', 'children'],
  maxRows: 10_000,
  maxBytes: 8 * 1024 * 1024,
  timeoutMs: 30_000,
});
// Provision compatible, empty destination tables separately.
await applyChangeset(destination, snapshot.changeset, {
  tables: ['parents', 'children'],
  deliveryId: 'source-42:initial-seed',
});
```

The entire read uses one owned transaction through `ChangesetTarget`, including
all tables and all pages. A nested target supplies a savepoint and therefore the
parent's snapshot; its result remains provisional until that parent commits.
No application DML, TEMP journal, trigger installation, PRAGMA mutation, or
unrelated row rewrite is performed. Source triggers are allowed but never fired;
`recursive_triggers=ON` is not required for this read-only operation. Incremental
`captureChangeset` keeps its existing mutation-observation restrictions.

## Schema and row semantics

Select 1..64 existing ordinary tables in `main`, each with 1..256 nongenerated,
visible columns and a declared primary key of 1..16 columns. Names are captured
before asynchronous admission; SDK/system tables are rejected. Views, virtual
or shadow tables, generated columns, and missing/incompatible keys fail before
row-image collection. Empty tables need a valid schema but add no wire records.

A selected table containing any NULL primary-key component is rejected. SQLite's
session format cannot represent such rows; omitting them would silently produce
an incomplete baseline. Table order follows the supplied list, so give parents
before children for immediate foreign keys. The helper does not disable or defer
receiver constraints, infer a topological ordering, or resolve cyclic references.
Use matching schemas and an explicit constraint policy. Existing conflicting
receiver rows fail under the normal application conflict policy; a seed is not
an upsert or a destructive replacement of a live database.

Reads follow the actual primary-key index, including declared key order,
ASC/DESC directions and collations. INTEGER PRIMARY KEY aliases use direct
ordering. Disjoint prefix ranges provide continuation without OFFSET or a broad
OR predicate. Each size/value page is at most 32 rows. Integers use typed decimal
projections, text uses encoding-aware byte projections, REAL and BLOB retain
their storage classes, and binary keys are bound rather than interpolated.

## Bounds and consistency

`maxRows` defaults to 10,000 (maximum 100,000), `maxBytes` to 8 MiB (maximum
64 MiB), and `maxCells` to 100,000 (maximum 1,000,000). These budgets apply across
all selected tables. SQL size projections check a page's row/slot/image costs
before its values cross to JavaScript, including oversized single BLOBs. The
`limits` option separately bounds codec output. Limits, invalid adapter results,
cancellation and deadlines reject the entire operation, never return a partial
seed. Budgets describe accounted images, not total SQLite memory, file size or
RSS. A whole bounded changeset is materialized; this is not streaming transfer.

All source reads must use the target's same consistent transaction. Another
connection's commit cannot change the source view halfway through a seed. This
does not make later source writes appear in already-returned bytes. Standalone
snapshot output is not a durable outgoing message, and source changes after the
snapshot need an ordered incremental delivery path. Browser snapshot databases
still require explicit durability confirmation; a memory commit is not a saved
checkpoint. A SQL snapshot cannot manufacture missing native storage guarantees.

## Executed verification

On Node 22.16.0 / SQLite 3.49.1, 37 tests pass with no failures or skipped tests.
The tests compare INSERT snapshots against native session output and apply/invert
them with SQLite itself. They include mixed composite keys, both rowid layouts,
int64 extremes, REAL/BLOB/text and UTF-16, quoted identifiers, source triggers,
all-table preflight, NULL-key refusal, budgets, malformed adapter replies and
cancellation. A second file-backed connection commits changes between page reads;
the seed retains the original view across both pages and tables. A 10,000-row
mixed-direction-key test checks every paged query plan for temporary sorting and
checks actual page sizes. Existing capture coalescing and rollback are exercised.

```sh
node --experimental-loader=./packages/sdk/tests/helpers/source-loader.mjs \
  --test packages/sdk/tests/changeset-snapshot.test.mjs
```

Strict TypeScript 5.8.3 checks pass against the actual capture, codec and apply
source files. Full SDK/worker, Rust/WASM, browser persistence and power-loss
certification were not run. These are reference-engine SQL tests, not claims
that every FrankenSQLite runtime supports all exercised SQL shapes.

## Atomic first-message bootstrap

`ChangesetOutbox.bootstrap` retains the snapshot and delivery identity in the
same source transaction that read all selected rows. It makes the seed the
first outbox entry; subsequent `record` calls follow it in sequence. There is no
window between a separate snapshot read and its eventual outbox insertion.

```ts
import { ChangesetOutbox, ChangesetDeliveryPump } from '@frankensqlite/sdk';

const outbox = new ChangesetOutbox(source);
const seed = await outbox.bootstrap({
  deliveryId: 'source-42:initial-seed',
  tables: ['parents', 'children'],
  maxRows: 10_000,
});
// Only enable the incremental writer workflow after bootstrap succeeds.
await source.execute('PRAGMA recursive_triggers=ON');
await outbox.record(tx => tx.execute(
  'UPDATE parents SET name=? WHERE id=?', ['revised', 12n],
), { deliveryId: 'source-42:change-1', tables: ['parents', 'children'] });

// Use the existing confirmed receiver / HTTP transport on the other end.
const pump = new ChangesetDeliveryPump(outbox, {
  receiverId: 'replica-42',
  deliver,
  confirmSource: () => source.checkpoint(), // For this snapshot-backed source.
});
await pump.run();
```

Create matching empty destination tables before delivery. DDL, indexes, triggers,
extra destination rows, non-selected source tables and automatic schema migration
are outside this seed. The bootstrap and incremental table set must cover every
application table that needs replication. All changes after the seed's read
boundary must go through `outbox.record`, including later writes inside an
enclosing source transaction. A snapshot does not observe unrecorded changes.
Source/receiver constraints retain their normal behavior; source mutations still
have capture's existing trigger restrictions. Do not accept omissions when an
exact baseline is required.

### First-use and replay contract

Bootstrap accepts only an unused outbox, not just one with no pending messages.
Both retained rows and the AUTOINCREMENT history are checked in the transaction.
An outbox that previously recorded or acknowledged work cannot append a baseline,
even after every acknowledged entry has been explicitly forgotten. A fresh
bootstrap must receive sequence 1. A competing incremental operation that wins
first makes bootstrap reject; there is no automatic replay, global writer lock,
or attempt to insert a snapshot behind that operation. Initialize bootstrap
before enabling incremental producers. The target's transaction/conflict engine
must correctly enforce the atomic read/write decision.

The retained `deliveryId` distinguishes a snapshot from a callback operation.
A retry of the same ID, table set and indirect policy returns `replayed: true`
and its original delivery metadata. It verifies retained payload bytes, but
never re-reads newer application rows or invokes a source callback. Changing the
table set or using a callback operation's ID rejects. Table-list reordering on a
retry returns the original payload order; it does not regenerate the seed.
Capture budgets constrain new collection; replay uses the existing outbox's
bounded retained-payload validation rather than resnapshotting.

Once acknowledged, the payload is reclaimed by the normal outbox machinery;
bootstrap retry still returns the acknowledged identity without regenerating
bytes. Forgetting that identity ends its retry protection, but does not reset
outbox history or authorize reseeding. The schema and reserved tables are
trusted application state: do not drop/recreate them, reset sqlite_sequence,
or change the fixed destination to circumvent these checks. Backups restore
their own historical delivery state, not later acknowledgements.

An empty source produces a valid empty changeset and still retains sequence 1.
A failed schema check, image/codec/outbox limit, cancellation, transaction conflict
or enclosing rollback leaves no committed seed. A lost source commit response
is an unknown outcome: retry the same bootstrap ID. A retained seed recovers;
rolled-back work may collect afresh. Once a seed is durable, newer captured
changes remain ordered after it. Nested results are provisional until the outer
transaction commits, and no returned metadata alone confirms durable storage.

The seed passes unchanged through the existing apply/inbox/delivery protocol.
Receiver replay prevents repeated INSERTs when an acknowledgement is lost.
Source and receiver snapshot stores still require explicit checkpoints, and the
pump's confirmation callbacks must target the correct databases. No transport,
checkpoint, automatic retry, source authentication or native replication engine
is created by bootstrap. Large datasets exceeding one bounded changeset still
need a separately designed multi-message snapshot protocol; they are rejected,
not silently truncated or divided across inconsistent transactions.

### Combined executed verification

The snapshot/bootstrap suite passes **69/69 tests**, with zero failures or skips,
on Node 22.16.0 / SQLite 3.49.1. Strict TypeScript 5.8.3 checks include the actual
capture, codec, application and outbox sources. Bootstrap tests use real source
and receiver SQL, the SDK application/inbox path, and native SQLite session
comparison. Twelve deterministic workloads seed existing rows, capture later
upserts, deliver in order and compare final contents.

Additional tests cover empty seeds, source/receiver lost acknowledgements,
receiver constraint rollback, identity/method/scope collisions, acknowledged and
forgotten history, budgets, cancellation, nested commit/rollback and corrupt
snapshot payloads. Two file-backed connections overlap at seed collection; only
one bootstrap commits. A competing incremental writer can win, but the stale
bootstrap then fails instead of publishing behind it. Separate child processes
are SIGKILLed before source commit and after commit before response. Reopened
files recover the appropriate first-message decision and deliver without a new
source snapshot for the committed case. Process death is not power-loss proof.
No new HTTP, browser, full SDK/worker or Rust/WASM certification is claimed.
