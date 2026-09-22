# Confirmed ordered changeset delivery

This integration uses the existing `ChangesetOrder` ledger. It does not replace
its schema, historical replay behavior, independent head, or source binding.
The source incarnation is `sourceId` in the ledger and `streamId` on the wire.

## Receiver

```ts
import { ChangesetOrder, applyChangeset, createOrderedChangesetReceiver } from '@frankensqlite/sdk';

const order = new ChangesetOrder(db, {
  receiverId: 'east', sourceId: 'source:incarnation-1',
});
// Trusted enrollment only, after agreeing the initial data/schema:
await order.initialize();
// Normal reopen validates existing state; it never initializes from peer input.
const receiver = await createOrderedChangesetReceiver(order, {
  apply: (target, message, options) => applyChangeset(target, message.changeset, {
    ...options, tables: ['notes'], deliveryId: message.deliveryId,
  }),
  confirmCommit: confirmReceiverDatabase,
});
const receipt = await receiver.receive(authenticatedOrderedEnvelope);
```

The application MUST use and await exactly one transaction on the supplied
`target`. That scope shares the ledger's outer SQL transaction: rows, inbox
receipt, optional rebase-journal writes, and order advancement commit together.
The adapter validates returned decisions against the transaction's result;
premature responses or ignored targets fail inside the transaction. Admitted
SQL is drained before the operation settles, including on failure.

`confirmCommit` must confirm the SAME top-level database, and is called only
AFTER the outer SQL transaction resolves. Never put a `ChangesetReceiver.receive`
call, checkpoint, external acknowledgement, or network effect in `apply`: that
would try to confirm before the enclosing order commit. A rebase journal may
instead be constructed on the supplied target and its `apply` method used
there. Both application callbacks and SQL adapters are trusted, not sandboxed.

An exact historical replay returns the existing ledger's original counts and
skips application, but still awaits confirmation. A failed/lost confirmation
therefore retries the same ID, sequence and bytes, not new SQL. Cancellation
inside SQL rolls back; cancellation after SQL commit can withhold the ACK but
cannot undo that commit. Confirmation is awaited rather than raced against a
timer. One receiver refuses overlapping receives through confirmation.

Wire messages extend the existing `fsqlite-changeset-v1` envelope with:

```ts
order: {
  protocol: 'fsqlite-ordered-changeset-v1',
  streamId: 'source:incarnation-1',
  sequence: '1', // canonical positive int64 decimal, never a JSON number
}
```

Confirmed receipts return that same immutable order evidence alongside the
ordinary identity, digest, length and application counts. Admission rejects
missing/order-downgraded messages, wrong streams/receivers, noncanonical
sequences and digest mismatches. Buffers are copied before asynchronous work;
limits use intrinsic typed-array lengths, not shadowable properties. The
receiver defaults to 8 MiB and has a 64 MiB maximum; ledger limits still apply.
These are message admission bounds, not process RSS guarantees.

Authenticate peers outside this API. Routing identities and SHA-256 are not
sender authentication. Provision a new source incarnation for rebuilt sources;
never reuse an old source sequence for new work. An in-memory SQL commit is
not durable merely because the ledger exists. Snapshot modes need successful
checkpoint/recovery of the same database before confirmation. The order must
own the top-level commit, not be nested in an uncommitted outer transaction.

## Verification

```sh
node --experimental-transform-types \
  --experimental-loader=./packages/sdk/tests/helpers/fanout-source-loader.mjs \
  --test packages/sdk/tests/changeset-ordered-delivery.test.mjs
```

The suite runs the production ledger and adapter against Node's actual SQLite
transactions and native session changesets, including file reopen and deferred
COMMIT failure. Its SQL application callback is a reference adapter, not the
existing production SDK `applyChangeset`, receiver or pump. It does not certify
the Rust/WASM engine, browser storage, full SDK build or power-loss behavior.
