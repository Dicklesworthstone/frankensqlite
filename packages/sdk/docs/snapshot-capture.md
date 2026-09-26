# Capture transactions with application triggers

`captureSnapshotChangeset` captures the net row changes of one callback, including
writes performed by existing BEFORE/AFTER/TEMP triggers, INSTEAD OF view triggers,
and foreign-key cascades. It reads selected tables before and after the callback
inside the same owned transaction. It adds no observation triggers, does not alter
`recursive_triggers`, and requires no native Session hook.

```ts
import { captureSnapshotChangeset } from '@frankensqlite/sdk';

const captured = await captureSnapshotChangeset(source, async tx => {
  // Existing source triggers may update inventory and append audit rows.
  await tx.execute('UPDATE orders SET status=? WHERE id=?', ['paid', 42n]);
  return 42n;
}, {
  tables: ['orders', 'inventory', 'audit'],
  maxRows: 10_000,
  maxBytes: 8 * 1024 * 1024,
  timeoutMs: 30_000,
});
```

This is an explicit alternative to `captureChangeset`, not a silent fallback.
Ordinary capture still uses its first-touch journal and rejects application
triggers. Snapshot capture scans ALL selected rows twice, including unchanged
rows. Use it only when that bounded full-scope cost is appropriate. Large tables
need a native observation path; this method does not make small updates to an
arbitrarily large table cheap.

## Transaction and row contract

Both reads, callback work, net-change encoding and validation share one source
transaction or an enclosing owner's savepoint. A later limit, SQL, encoding or
schema error rejects the operation, requiring the owner to roll back its writes.
A nested result is provisional until the outer transaction commits. The returned
bytes alone are not retained outgoing work or a storage checkpoint.

The callback receives a scoped SQL executor. Already admitted calls settle before
after-image collection; a saved executor rejects after the callback exits. ANY
failed admitted SQL rejects capture, even if the callback catches that failure.
Cancellation and deadlines cover both scans, the callback, its admitted work and
encoding. They wait for started SQL to settle instead of racing rollback. Trusted
callbacks must not commit/roll back the owning transaction, change schemas or
connection policy, reenter the owner, or run unrelated SQL through another handle
on the same connection. This is transaction composition, not a SQL sandbox.

Every selected table must be an ordinary existing `main` table with visible,
nongenerated columns and a declared non-NULL primary key, as for snapshot export.
All tables are preflighted before the callback. Select every affected table whose
rows need replication; unselected trigger effects are not included. Primary keys
are compared by storage class and exact value, not coerced strings. Binary and
composite keys, key changes, full deletes, empty tables, signed int64, REAL, BLOB,
and encoding-aware NUL/BOM/Unicode text use the existing snapshot reader and codec.

Repeated updates coalesce. A temporary insert then delete, or restoring the
original values, yields no net record. Deletions precede other changes within a
table, and tables follow the requested order. `beforeRows` and `afterRows` count
scanned rows, NOT touched rows. There is intentionally no `touchedRows` field:
observing only two states cannot recover intermediate touches or events.

The output is a normal reversible SQLite changeset, not an SQL trace or an audit
log. The `indirect` option applies uniformly to the whole scope (default false).
This does not infer native Session's per-change trigger-depth provenance. Receiver
constraints still apply. Do not blindly recreate source business triggers or
cascading actions on a replica and execute those effects a second time while also
applying their captured records. Provision appropriate replica behavior and use
an explicit transaction-level foreign-key policy for dependent/cyclic records;
this helper neither disables constraints nor silently omits conflicts.

## Resource contract

Each individual before/after scan is limited to `maxRows` (default 10,000, maximum
100,000), `maxBytes` (default 8 MiB, maximum 64 MiB of accounted row images and
intermediate chunk bytes), and `maxCells` (default 100,000, maximum 1,000,000).
The same limits apply independently to BOTH scans, including unchanged rows.
The optional `limits` bounds final changeset output separately. Crossing any limit
fails rather than emitting a partial transaction.

Existing indexed keyset reads transfer at most 32 images per page, admitting SQL
size projections before transferring their values. The full before image is
retained for matching; after images are consumed in bounded chunks. Changed row
images, exact encoded key indexes, page buffers, codec copies and output coexist.
These are bounded logical-data budgets, not an RSS, database-engine memory, or
zero-copy guarantee. The method pins one source snapshot for its entire lifetime.

## Executed verification

Run with Node 22.16.0 or a compatible runtime:

```sh
node --experimental-transform-types \
  --experimental-loader=./packages/sdk/tests/helpers/production-source-loader.mjs \
  --test packages/sdk/tests/changeset-snapshot-capture.test.mjs
```

The 45 tests execute the actual snapshot reader, capture, codec and application
modules over reference SQLite 3.49.1. Native Session comparisons independently
check row operations and images; comparisons deliberately normalize the indirect
flag because this API uses the documented whole-scope policy. Cases cover
self-updating/generated-key triggers, BEFORE/TEMP and view triggers, cascades,
all three database encodings, twelve mutation workloads, native apply/inversion,
mixed keys, bounded pages, rollback, resource limits, deadlines, executor lifetime,
and overlapping file-backed source snapshots.

Strict TypeScript 5.8.3 checks include actual transitive source dependencies.
Reference SQL ownership is not execution of FrankenSQLite's Rust/WASM/MVCC or
full SDK/worker packaging. Browser storage, production deployment and physical
power-loss qualification are not claimed. Default capture, native writer settings,
wire formats and dependency lists are unchanged.

## Retain trigger-driven work atomically in the outbox

`ChangesetOutbox.recordSnapshot(work, options)` runs the same capture inside the
outbox's existing publication transaction. Source writes, selected trigger/cascade
results, the outgoing payload and operation identity commit together. A failure
in capture, final payload admission, metadata validation or COMMIT leaves no
committed operation. It is not a separate snapshot followed by a later enqueue.

```ts
const result = await outbox.recordSnapshot(async tx => {
  await tx.execute('UPDATE orders SET status=? WHERE id=?', ['paid', 42n]);
  return 42n;
}, {
  deliveryId: 'source-42:pay-order-42',
  tables: ['orders', 'inventory', 'audit'],
  maxRows: 10_000,
});
```

`OutboxSnapshotRecordOptions` adds the usual stable, source-qualified `deliveryId`
to `SnapshotCaptureOptions`. Results use the existing `OutboxRecordResult<T>`:
new operations return `value` and delivery metadata; replays return the retained
metadata without a callback result. The before/after scan counts are available
from standalone capture, not the outbox result.

The internal capture scope distinguishes snapshot recording from ordinary journal
recording and either kind of bootstrap. Reusing an ID across methods, table sets
or indirect policies rejects. Reordering the same table list on a retry is allowed
but preserves the original payload order. A retained retry verifies pending bytes,
never re-reads newer application rows, and never repeats business triggers. Once
acknowledged, its reclaimed payload is not regenerated. Explicit identity forgetting
has the same consequences as existing outbox forgetting: it ends retry protection;
never recycle an old business-operation identity for new work.

Both recording strategies share the existing capacity, hash, metadata and fanout
guards. `maxEntries` and `maxPayloadBytes` apply independently of capture budgets.
New work refuses a full outbox before scanning application data. Required replicas
retain shared bytes until all have acknowledged them. Triggered attempts to mutate
the roster/progress fail with the business transaction. The payload is an ordinary
changeset and requires no new delivery protocol or receiver API. A replica must not
execute source-side business effects a second time; the row-only receiver schema
and appropriate FK policy remain application responsibilities.

There is no implicit network call, automatic retry, background worker, checkpoint,
source-ID generation or weakening of native concurrent writers. A lost source
commit response is uncertain: retry the SAME retained operation ID. Browser
snapshot-backed sources and receivers still need genuine same-database storage
confirmation using the existing delivery APIs. Nested outbox results remain
provisional until the outer owner commits.

### Combined executed coverage

The two suites pass 82 tests with no failures/skips: 45 capture tests and 37 outbox
integration tests. Production snapshot/capture, codec, applyChangeset, outbox,
store and fanout modules execute without backend substitution. Typechecking covers
the actual transitive sources, not declaration fixtures. The new outbox cases
exercise trigger-driven records in UTF-8/UTF-16LE/UTF-16BE databases, native row
application, bootstrap-to-incremental ordering, exact replay and method collisions,
empty payloads, slow replicas, rollback, capacity, cancellation and delayed SQL.

Two source connections are forced to overlap before competing read-to-write
promotions. Eight child processes are actually SIGKILLed under WAL/DELETE journals:
after trigger work, after payload insertion, before COMMIT and after COMMIT before
response. Fresh connections recover all-or-none publication and reuse committed
operation bytes without repeating source triggers. Other cases lose receiver
responses and recover through the real applyChangeset inbox. These are process
death and reference-SQL tests, not power-loss, Rust/WASM/MVCC, browser persistence,
HTTP deployment or full SDK packaging qualification.

```sh
node --experimental-transform-types \
  --experimental-loader=./packages/sdk/tests/helpers/production-source-loader.mjs \
  --test packages/sdk/tests/changeset-snapshot-capture.test.mjs \
  packages/sdk/tests/changeset-outbox-snapshot.test.mjs
```
