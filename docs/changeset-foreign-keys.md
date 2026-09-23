# Foreign-key-safe changeset transactions

`withDeferredForeignKeys(target)` is an explicit transaction policy for applying
complete changes that are foreign-key-valid at the end, but not at every
intermediate row. For example, a changeset may insert a child before its parent,
delete a parent before its child, or change both sides of a cyclic reference.
The existing immediate-constraint behavior remains the default.

```ts
import { applyChangeset, withDeferredForeignKeys } from '@frankensqlite/sdk';

await db.execute('PRAGMA foreign_keys=ON');
const target = withDeferredForeignKeys(db);
await applyChangeset(target, remoteBytes, {
  deliveryId: 'trusted-source:operation-42',
  tables: ['parents', 'children'],
});
// Snapshot persistence still needs the SAME database's checkpoint barrier.
```

The target must implement real owned transaction/savepoint semantics, roll back
on rejection, and exclude unrelated SQL during the callback. This adapter does
not issue BEGIN, COMMIT, or ROLLBACK. Application rows and the existing receipt
remain in one transaction. The same target policy can surround patchset
application, a rebase journal, or an ordered-receiver ledger.

## Ordered delivery composition

Wrap the outer ledger target, not a separate connection:

```ts
import {
  ChangesetOrder, applyChangeset, createOrderedChangesetReceiver,
  withDeferredForeignKeys,
} from '@frankensqlite/sdk';

const ledger = new ChangesetOrder(withDeferredForeignKeys(db), {
  receiverId: 'replica-42',
  sourceId: 'trusted-source:incarnation-1',
});
await ledger.initialize(); // Explicit trusted provisioning, never wire enrollment.
const receiver = await createOrderedChangesetReceiver(ledger, {
  apply: (scoped, message, controls) => applyChangeset(scoped, message.changeset, {
    ...controls,
    tables: ['parents', 'children'],
    deliveryId: message.deliveryId,
  }),
  confirmCommit: confirmSameDatabaseCommit,
});
```

The final FK check is inside the outer transaction, after its application and
order/inbox/journal writes. Commit confirmation stays outside that transaction.
Neither this policy nor a SQL COMMIT turns a memory database into durable storage.

## Enforcement and restoration

Before changing deferral, the scope requires `foreign_keys=ON`, reads the incoming
`defer_foreign_keys` setting, and checks every attached schema, including main and
TEMP. Pre-existing violations are rejected before changing either setting. This
includes a parent's declared-deferred violations while the pragma is OFF: the
scope must not erase existing constraint debt when it later restores OFF.

The scope enables deferral only when necessary. After the callback, it drains
admitted SQL, checks that enforcement settings and the schema roster have not
changed, and verifies that no FK violations remain in any schema. An unresolved
violation rejects the complete transaction; it is not an omission-policy event.
Schema and SQL errors propagate rather than silently weakening enforcement.

The original deferral setting is restored before the callback returns to the
transaction owner. A nested successful scope therefore does not make the rest
of its parent transaction unexpectedly deferred. An incoming ON setting stays
ON until SQLite's normal outer COMMIT/ROLLBACK resets it. Existing defaults and
connection-wide `foreign_keys` enforcement are never switched off.

Callbacks must not change connection pragmas, ATTACH/DETACH databases, or issue
transaction control. The scope checks final state, but it is not a SQL sandbox
and does not undo connection-state or external side effects such as ATTACH.
Use trusted application callbacks and the target's normal ownership rules.

## Cancellation and failures

Signals and deadlines are cooperative: an already admitted SQL operation must
settle before validation, restoration, or rollback can finish. The adapter does
not race a commit against a timer. Retained callback executors refuse later SQL;
caught or unawaited SQL errors still reject the whole owned scope. Simultaneous
uses of wrappers around the same target are rejected, not placed in an unbounded
queue. Different targets remain independent; no native/global writer lock is added.

Restoration is attempted without the adapter's cancellation checkpoints. A target
may nevertheless reject cleanup SQL once its own scope has been cancelled. In
that case `ERR_FSQLITE_FOREIGN_KEY_STATE` includes the original `cause` and a
`cleanupErrors` array. Await rollback and reconcile or discard the connection
before reuse; do not assume the pragma was restored. A real transaction owner
must roll back failed application even when cleanup fails.

No delivery is retried automatically. A lost response after COMMIT is uncertain,
not evidence of rollback: reconcile the original delivery ID and stored receipt.

## Requirements, bounds, and verification

The SQL target must implement SQLite's `foreign_keys` and `defer_foreign_keys`
pragmas, `pragma_database_list()`, and the schema-aware two-argument
`pragma_foreign_key_check(NULL, schema)` table-valued function correctly. Unknown
or malformed pragma acknowledgements reject. Schema metadata is limited to 127
entries plus a single overflow sentinel; violation queries return at most one
row per schema. Scanning all foreign keys can still be expensive. There is no
claim of bounded engine work, heap/RSS, or native Rust/WASM qualification.

```sh
node --experimental-transform-types --test \
  packages/sdk/tests/changeset-foreign-keys.test.mjs
```

The suite executes the production adapter against actual Node SQLite transactions,
nested savepoints and files. It covers cyclic/composite/WITHOUT ROWID keys,
RESTRICT/CASCADE actions, main/TEMP/attached violations, existing deferred debt,
callback and commit failure, cleanup uncertainty, cancellation/drain, lost
acknowledgements and reopen. One fixture compares final rows to actual native
SQLite session application. The application callbacks and transaction owner are
reference fixtures: this is not an end-to-end run of production apply/order/HTTP,
FrankenSQLite Rust/WASM, browser storage, or power-loss certification.
