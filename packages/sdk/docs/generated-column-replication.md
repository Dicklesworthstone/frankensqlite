# Replicate writable fields and recompute generated columns

Set `generatedColumns: "recompute"` explicitly to enable ordinary tables with
VIRTUAL or STORED generated columns in `captureChangeset`, `snapshotChangeset`,
`streamSnapshotChangesets`, `captureSnapshotChangeset`, `applyChangeset`,
`applyPatchset`, and `ChangesetBootstrapReceiver`. Outbox `record`,
`recordSnapshot`, `bootstrap`, and `bootstrapChunks` inherit the capture option.
The default remains rejection of generated schemas; unknown policy values fail
before transaction admission. Existing callers retain their fail-closed policy.

```ts
const captureOptions = {
  tables: ['orders'],
  generatedColumns: 'recompute' as const,
};
// Both databases already have compatible schemas. For example:
// CREATE TABLE orders(
//   total INTEGER AS(quantity * price) STORED,
//   id INTEGER PRIMARY KEY, quantity INTEGER, price INTEGER
// );
const captured = await captureChangeset(source, async tx => {
  await tx.execute('UPDATE orders SET quantity=? WHERE id=?', [4n, 12n]);
}, captureOptions);
await applyChangeset(destination, captured.changeset, {
  ...captureOptions, deliveryId: 'source-42:order-12-revision-7',
});
```

This follows native SQLite Session's writable-column layout, not physical table
slots. Generated values do not occupy changeset columns. The receiver writes
base columns only; its own schema computes derived values and enforces their
CHECK, UNIQUE, NOT NULL and foreign-key constraints normally. No special SQL
permission, disabled constraint, alternate binary format or generated-value
assignment is introduced. Generated constraint failures roll back the owned
scope, including its inbox/outbox or complete bootstrap decision.

## Logical versus physical column order

Source schema inspection preserves both the physical `table_xinfo` column id and
the packed writable Session position. Capture journals and row images include
only writable columns. Snapshot keyset pagination translates physical
`index_xinfo` ids back to those positions, preserving actual primary-key index
order, mixed ASC/DESC directions and collations. An INTEGER PRIMARY KEY after a
generated column still refers to the correct writable key; composite WITHOUT
ROWID tables retain their declared key order.

Application maps the incoming key mask and old/new images against writable
columns in declared order, skipping generated columns wherever they occur. Extra
trailing writable receiver columns keep the existing default-value contract.
Having enough physical columns is not sufficient: too few writable columns
reject before row application. Hidden virtual-table columns, virtual/shadow
application tables, missing keys and generated key components remain unsupported.
Generated columns count toward existing physical schema limits. Existing row,
cell, byte and chunk budgets are not increased.

## Bootstrap and ordered delivery

Enable capture at the source and recomputation on the provisioned destination:

```ts
await outbox.bootstrapChunks({
  deliveryId: 'source-42:baseline', tables: ['orders'],
  generatedColumns: 'recompute', chunkRows: 1024,
});
const receiver = new ChangesetBootstrapReceiver(destination, {
  receiverId: 'replica-east', tables: ['orders'],
  generatedColumns: 'recompute',
  orderedSourceId: 'source-42:incarnation-1',
  confirmCommit: () => destination.checkpoint(),
});
```

The existing transfer/HTTP bootstrap protocol is unchanged. The receiver's mode
is trusted local configuration, not a field the sender can enable over the wire.
All destination tables must still be compatible and empty, including selected
empty tables. The complete baseline and optional ordered prefix commit together;
a bad generated value in a later chunk cannot publish an earlier chunk's rows.
For ordered increments, pass the same option to `applyChangeset` inside the
existing ordered receiver's application callback. The ordinary `ChangesetReceiver`
convenience wrapper does not yet expose this opt-in; do not pass it an unknown
option and expect generated schemas to be admitted.

Receipt and outbox replay recover prior retained decisions/bytes; they do not
regenerate values from a new source snapshot or reapply an installed baseline.
The option controls fresh schema admission, not delivery identity. Like capture
budgets, changing it does not regenerate an existing retained operation. Replaying
already committed history does not execute generated expressions again.

## Boundaries

Source and receiver generated expressions, collations, functions, affinities and
constraints must be provisioned compatibly by the application. The wire does not
ship DDL or compare expression definitions. Hashes bind writable bytes, not the
schema or its computed output. A different receiver expression can legitimately
compute a different value without a changeset DATA conflict; use compatible,
trusted schemas rather than treating this as an automatic schema migration.

Snapshot image budgets account for writable fields, not generated expression
execution, index maintenance, SQL memory or total process RSS. A large virtual
value can be absent from a small transmitted changeset while still being costly
to compute at the destination. Trigger-driven snapshot capture remains two full
selected-table scans; the journal capture path still rejects application triggers.
Generated-column support does not remove either strategy's ownership restrictions.
No automatic transport retry, checkpoint, pruning or global writer lock is added.

## Verification

The dedicated suite uses native SQLite Session bytes as an independent producer
and applies captured changesets both through the SDK and native SQLite. It covers
interleaved STORED/VIRTUAL columns, writable-only tables on the other end, all
three database encodings, int64/NUL/BOM/Unicode data, insert/update/delete/inversion,
patchsets, 101-row mixed-direction composite-key paging, both rowid layouts,
trigger-driven snapshot outbox records, receiver-only defaults, CHECK/UNIQUE
rollback, late-chunk bootstrap failure, file reopen and ordered sequence N+1.
Actual paged query plans and returned page bounds are checked. Default rejection,
invalid mode values, malformed schema replies and option ownership are retained.

```sh
node --experimental-transform-types \
  --experimental-loader=./packages/sdk/tests/helpers/production-source-loader.mjs \
  --test packages/sdk/tests/changeset-generated-columns.test.mjs
```

The loader resolves imports without substituting production modules. These are
SDK-component tests with Node 22.16.0 / reference SQLite 3.49.1 SQL ownership,
not FrankenSQLite Rust/WASM/MVCC execution or full FrankenDB/worker packaging.
Native generated-expression parity, browser persistence, production TLS and
physical power-loss behavior are not certified. Explicit same-database storage
confirmation remains required for snapshot-backed sources and receivers.
