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
