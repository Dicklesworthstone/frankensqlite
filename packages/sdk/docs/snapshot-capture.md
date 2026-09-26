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
