# Generated columns and deferred foreign keys through replication receivers

`ChangesetReceiver` accepts the same explicit local application policies as
`applyChangeset`: `generatedColumns: "recompute"` and `foreignKeys: "defer"`.
These work both with the ordinary inbox and with a configured rebase journal.
`ChangesetRebaseJournal.apply()` now forwards both options to its owned application
instead of silently dropping options advertised by `RebaseJournalApplyOptions`.

```ts
const receiver = new ChangesetReceiver(destination, {
  receiverId: 'replica-east',
  tables: ['child', 'parent'],
  generatedColumns: 'recompute',
  foreignKeys: 'defer',
  rebaseJournal: { journalId: 'replica-east:conflict-history' },
  confirmCommit: () => destination.checkpoint(),
});
```

The destination must already have compatible trusted schemas and foreign-key
enforcement enabled. For a snapshot-backed database, confirmation must checkpoint
or recover this SAME top-level database. Already durable SQL owners can supply an
explicit confirmation implementation consistent with their commit contract. A
no-op on an in-memory database does not establish durability.

Pass the configured receiver to the existing `createChangesetHttpHandler` when
using HTTP. Authentication and source-namespace authorization remain host policy.
There is no new framing or wire version. Neither a sender envelope nor HTTP input
can enable or replace these policies: the receiver captures them once at
construction. Mutating the caller's options afterward cannot change admission.
Unknown policy values reject before SQL rather than selecting a fallback.

## Preserve one application decision

Generated-column recomputation uses the existing packed writable-field mapping;
SQL computes the derived values and still enforces generated CHECK, UNIQUE and
foreign-key constraints. No generated expression or DDL is received from a sender.
Missing opt-in retains the ordinary generated-column rejection on fresh work.

Foreign-key deferral reuses `withDeferredForeignKeys`, not a second constraint
implementation. It requires enforcement already ON and a clean main/TEMP/attached
entry and exit. A message may temporarily insert a child before its parent or
construct a cycle, but unresolved references cannot produce a successful receipt.
Rows, inbox receipt and rebase decisions share the same transaction and final
check. A constraint or journal-capacity failure rolls them all back. Entry/exit
checks scan schemas; this is not constant-cost validation of only changed rows.

Direct journal application has the same option contract:

```ts
await journal.apply(bytes, {
  deliveryId: 'source-42:transaction-108',
  tables: ['child', 'parent'],
  generatedColumns: 'recompute',
  foreignKeys: 'defer',
});
```

The journal captures these options before asynchronous transaction admission.
It still owns the rebase hook, codec limits and conflict-decision retention.
Exact replay finds the original decision instead of invoking conflict callbacks
or inventing journal history. Foreign-key replay rechecks current cleanliness
before the receiver confirms storage. A failed or lost confirmation does not
prove rollback; reopen with the same local policy and retry the same message ID.
Application rows may already have committed. No automatic retry, native writer
lock, stream re-enrollment, retention expiry or source callback replay is added.

## Executed verification

```sh
node --experimental-transform-types \
  --experimental-loader=./packages/sdk/tests/helpers/production-source-loader.mjs \
  --test packages/sdk/tests/changeset-receiver-policy.test.mjs \
  packages/sdk/tests/changeset-journal-encoding.test.mjs
```

The 34 receiver-policy cases and 31 journal-encoding cases pass on Node 22.16.0 /
reference SQLite 3.49.1. The same receiver-policy suite fails 28 of 34 cases before
policy propagation. No production replication backend is replaced by a fixture.
The adapter supplies reference SQLite SQL ownership, not FrankenDB execution.

Coverage includes all three database encodings, native Session input, generated
constraints, cyclic references, plain/journaled application, constructor ownership,
invalid options before SQL, lost confirmations and file reopen, original local
rebasing, capacity rollback and unchanged defaults. Actual source capture, outbox,
fanout and delivery pumps also run through two journaled receivers: a lost reply
is retried without source callbacks, and a slow receiver retains shared payloads
until it accepts the configured policy and acknowledges the messages.

Strict TypeScript checking follows all eleven actual transitive sources, without
declaration substitutes. This increment's transport tests use direct production
callbacks, not HTTP sockets. It does not qualify full SDK/worker packaging,
FrankenSQLite Rust/WASM/MVCC, browser snapshot persistence, production TLS/auth or
physical power loss. The wire protocol, dependencies, native concurrency defaults,
workflows and beads are unchanged.
