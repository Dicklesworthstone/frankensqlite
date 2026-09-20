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
is created by bootstrap. This single-message API rejects datasets exceeding one
bounded changeset. Use `bootstrapChunks` below for atomic source-side retention
of a multi-message seed; it does not provide atomic receiver-side visibility.

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

## Streaming one consistent snapshot into bounded changesets

`streamSnapshotChangesets(source, onChunk, options)` reads all selected tables
in one owned source transaction, but materializes only bounded pages and one
chunk rather than the complete seed. It awaits `onChunk` before continuing.
Each chunk contains one table's INSERTs, has a contiguous zero-based `index`,
and owns its bytes. An empty source emits one empty chunk. The resolved result
contains total `chunks`, `changes` and emitted wire `byteLength`.

```ts
const summary = await streamSnapshotChangesets(source, async chunk => {
  await staging.write(chunk.index, chunk.changeset);
}, { tables: ['parents', 'children'], chunkRows: 1024, chunkBytes: 1024 * 1024 });
// Only after success may the application finalize its own staged artifact.
await staging.complete(summary);
```

The sink and source must be trusted. Do not mutate/reenter the source from the
sink or publish chunks during collection as a completed baseline. Chunks remain
provisional until this operation and any enclosing transaction commit. A later
schema/limit/SQL error, sink failure or cancellation can invalidate already
emitted chunks; external sink effects cannot be rolled back by this API. Use
staging with an explicit completion decision, or transactional outbox storage.
An open stream pins one snapshot for its entire lifetime. There is no resumable
source cursor after failure: a new source transaction is a different snapshot.

`chunkRows` defaults to 1,024 (maximum 100,000). `chunkBytes` defaults to 1 MiB
(maximum 64 MiB), bounding both accounted images per chunk and encoded wire
bytes. SQL size pages contain at most 32 scalar sizes, and the following value
query is narrowed to a byte-fitting prefix. A single oversized row fails before
its values are transferred. A page and a chunk may coexist; encoding/copying has
additional bounded allocations. Sink-retained data, SQL execution memory and RSS
are not bounded by these options. Conservative wire accounting can emit smaller
chunks; `chunkRows`/`chunkBytes` are ceilings, not exact packing guarantees.

Streaming total limits are independent: `maxRows` defaults to 1,000,000 (maximum
10,000,000), `maxBytes` to 256 MiB (maximum 1 GiB of accounted row images),
`maxCells` to 10,000,000 (maximum 100,000,000), and `maxChunks` to 10,000 (maximum
100,000). Codec limits apply to each chunk; the single-image API's optional
`limits` is not a streaming option. Existing `snapshotChangeset` and capture
retain their old limits. Source schema/NULL-key checks, index ordering, typed
values, table order, and cooperative cancellation follow the same shared reader.
Cancellation and timeout wait for an already-started sink to settle; a sink that
ignores cancellation can delay completion. No background work is started.

The streaming suite passed 39/39 tests on Node 22.16.0 / SQLite 3.49.1, including
native apply/invert, 100,001 rows, a 70,000,000-byte BLOB dataset, bounded value
pages, concurrent source changes across tables/chunks, composite index order,
UTF-16/int64 preservation, cancellation drain and terminal failures. Actual
capture/codec/apply sources pass strict TypeScript 5.8.3 checks. These are
reference-SQLite tests, not Rust/WASM, browser, full-SDK or power-loss certification.

```sh
node --experimental-loader=./packages/sdk/tests/helpers/source-loader.mjs \
  --test packages/sdk/tests/changeset-snapshot-stream.test.mjs
```

## Atomic multi-message bootstrap

`ChangesetOutbox.bootstrapChunks` connects the streaming reader to persistent
outgoing delivery. All chunks, their individual digests and the final manifest
are retained in the SAME source transaction that reads all selected rows. There
is no committed partial prefix, no network call inside the transaction, and no
JavaScript array containing the complete seed. Subsequent `record` operations
follow the entire baseline in sequence.

```ts
const outbox = new ChangesetOutbox(source, {
  maxEntries: 10_000,
  maxPayloadBytes: 256 * 1024 * 1024,
});
const options = {
  deliveryId: 'source-42:baseline-1',
  tables: ['parents', 'children'],
  chunkRows: 1024,
  chunkBytes: 1024 * 1024,
  maxBytes: 256 * 1024 * 1024,
};
const seed = await outbox.bootstrapChunks(options);
// Only now enable producers using outbox.record(). Confirm the source before
// sending. The existing delivery pump performs that configured confirmation.
await pump.run({ maxDeliveries: 100 });
// Repeat bounded delivery runs as needed; never rerun source business callbacks.
const progress = await outbox.bootstrapChunks(options);
console.log(progress.acknowledgedChunks, progress.chunks, progress.complete);
```

Here `pump` must be configured for this exact outbox and a fixed, authenticated
destination, using the existing source/receiver confirmation contract. The API
does not manufacture a pump, schedule retries, or checkpoint either database.
The receiver must already have matching empty schemas. Include every table that
needs replication, and record all subsequent changes through the outbox. Table
order remains caller-selected, including immediate foreign-key dependencies.

### Atomic source publication, not atomic receiver installation

**Each chunk is applied in a separate receiver transaction. Keep the destination
staged or unavailable to application readers until the entire baseline has been
acknowledged and its receiver storage confirmed.** During delivery the receiver
may contain only part of the baseline. A later conflict does not undo earlier
chunks, and source rollback cannot undo remote SQL. This API does not implement
a staging-database swap, multi-message receiver transaction or atomic visibility
barrier. Do not accept omissions when a complete baseline is required.

The source snapshot and outbox append remain one transaction, so a concurrent
source writer can cause a storage conflict and abort the whole bootstrap. An
unused outbox is required. When an incremental writer wins first, bootstrap
fails rather than appending a baseline behind it. Start producers only after
bootstrap succeeds. Large source transactions can pin history and retain SQL
write buffers even though JavaScript row-image memory is chunked; the limits
are not a promise of small engine memory or short snapshot lifetimes.

### Identities, manifest and replay

The root `deliveryId` accepts 1..480 UTF-8 bytes without NUL. It identifies chunk
zero; later chunks use `${deliveryId}/chunk/${index}` with contiguous zero-based
indices. Do not use those derived delivery identities for other work. Chunk
sequences occupy 1 through N in a new outbox. Even an empty source retains one
empty chunk. Existing wire framing, receiver inbox receipts and HTTP transport
remain unchanged: each retained message is a normal INSERT-only changeset.

The root's existing scope field stores the manifest: total chunks, row changes
and encoded byte length. Each member binds its root, index and table/indirect
scope. Validation checks contiguous numeric sequences, matching identities,
totals and a contiguous acknowledged prefix. The result's `sha256` is the
digest of the FIRST chunk, not a cryptographic digest of the entire manifest.
Reserved SQL metadata and the target adapter are trusted; payload hashes do
not authenticate a source or protect against authorized metadata modification.

Calling `bootstrapChunks` again with the same root and table/indirect scope
returns `replayed: true`, the original totals and current acknowledgement
progress. It reads metadata in pages of at most 32 and verifies each still-
pending payload separately. It does not scan application rows or regenerate a
seed from newer data. Different chunk packing or collection budgets do not
rewrite retained work. A different operation type or capture scope rejects.
Missing, mismatched or corrupted retained chunks fail rather than being
silently replaced. Replay can therefore require reading all retained payloads;
it is bounded-memory validation, not constant-time status lookup.

`complete` means every chunk's SOURCE acknowledgement flag is set. It does not
prove that a snapshot checkpoint succeeded or that receiver application readers
are safe to admit. Use the existing confirmation/recovery rules on both ends.
After an uncertain source commit response, retry the same root: a committed
manifest recovers; a rolled-back bootstrap can collect a fresh snapshot. No
uncommitted source-read cursor survives process death.

### Capacity, acknowledgement and retention

The streaming row/image/cell/chunk limits above apply to collection. In addition,
all chunk identities count toward the outbox's `maxEntries`, and all pending
wire payloads count toward its `maxPayloadBytes`. A limit or error in ANY chunk
rolls back the complete source-side bootstrap. Configure the outbox's total
payload allowance separately from per-chunk limits; its default remains 64 MiB
and hard maximum 1 GiB. Choose `chunkBytes` within the receiver and transport's
individual-message limits. Each payload still obeys the existing codec bounds.

Acknowledging a chunk requires the exact delivery ID/digest and, for a later
chunk, an acknowledged predecessor. The normal pump starts with the oldest
pending message and stops on failure. Retrying after a lost receiver response
uses that chunk's retained inbox receipt, not another copy of the source work.
Do not manually acknowledge incremental messages ahead of an unfinished seed.

Individual `forgetAcknowledged` calls reject chunked-bootstrap members, because
deleting the root or a prefix would destroy complete-manifest recovery. After
ALL chunks are acknowledged, explicitly call
`forgetBootstrapChunks(seed.deliveryId, seed.sha256)` to remove the entire
group in one transaction. It preserves later incremental entries and the
AUTOINCREMENT history. Repeating cleanup after removal returns false; an
incorrect digest or pending group rejects. Forgetting ends retry protection,
but does not authorize reseeding used history. Never drop/recreate reserved
tables or reset sequence metadata to bypass these rules. Restoring backups
restores their older delivery and deduplication history.

### Executed chunked-bootstrap verification

The combined stream/bootstrap suite passes **74/74 tests**, with zero failures
or skips, on Node 22.16.0 / SQLite 3.49.1. Strict TypeScript 5.8.3 checks pass
against the actual outbox, capture, application and codec sources. Tests use
real SQLite transactions and native session application/inversion, plus the
SDK's actual inbox/application path.

The suite includes 100,001 rows, a 70,000,000-byte streamed BLOB dataset, and
68,000,000 bytes of BLOBs retained and replay-verified through a multi-page
outbox manifest with 1 MiB chunk limits. It checks sizes and indexed plans of
actual value pages, ordered seed-to-incremental convergence, lost source and
receiver acknowledgements, deferred commit errors, enclosing rollback,
capacity/cancellation failures, manifest corruption and group retention.

Two file-backed bootstrap writers overlap; a separate case lets an incremental
writer win while bootstrap holds its read snapshot. Child processes are actually
SIGKILLed midway through chunk insertion, immediately before COMMIT, and after
COMMIT before returning a response. Reopened files recover all-or-none source
publication and reuse committed seed bytes instead of rescanning newer rows.
These are process-death cuts, not simulated power-loss tests.

The larger cases also exposed and now guard two SQL name-resolution defects:
outbox pages order by the physical INTEGER sequence, not its projected TEXT
alias (which sorted 10 before 2); snapshot key expressions are table-qualified
so columns named `t0` or `x0` cannot resolve to typed-output aliases. The tests
cover ordinary incremental outbox pagination and both snapshot entrypoints,
not only the new chunked path.

Full SDK/worker builds, FrankenSQLite Rust/WASM execution, real browser storage,
atomic receiver visibility, native RaptorQ snapshots and physical power-loss
behavior are not certified by these reference-SQLite tests.
