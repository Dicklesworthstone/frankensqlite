# Bounded receipt retention for long-running ordered replicas

`ChangesetOrder.retireBefore(sequence)` explicitly releases old incremental
receipt identities without resetting the stream or changing application rows.
Previously the configured `maxEntries` was also a lifetime sequence ceiling:
a receiver reaching that sequence could not accept any further delivery.
Admission now counts physically retained identities rather than the sequence
number. Retention is opt-in; nothing expires or deletes receipts automatically.

```ts
const order = new ChangesetOrder(destination, {
  receiverId: 'replica-east', sourceId: 'source:incarnation-1', maxEntries: 10_000,
});
// Authenticate/confirm the source's acknowledgements and end its retry horizon
// BEFORE retiring old evidence. This API cannot establish that external fact.
const retention = await order.retireBefore(9_000n);
// For a snapshot-backed destination, confirm/recover this same database now.
await destination.checkpoint();
console.log(retention.removed, retention.retainedEntries);
// Ordinary ordered application continues with the original next source sequence.
```

The positive signed-int64 `bigint` boundary must not exceed the committed head.
The selected boundary receipt is retained as an independently recorded anchor,
along with every newer receipt and the genesis binding. All installed ordered
bootstrap receipts remain protected, even when below the requested boundary:
complete bootstrap replay and its original source acknowledgement must remain
verifiable after newer incremental work. A staged, uninstalled baseline cannot
be retired. Neither the head, source incarnation nor delivery numbering changes.

The frozen result contains `removed`, `retiredBefore`, `protectedThrough`, and
`retainedEntries`. `retiredBefore` is the earliest unprotected retained sequence;
sequences up to `protectedThrough` are exempt. Repeating a boundary, or requesting
an older one, returns zero removals and the current boundary. A request that
would remove no receipts does not create the optional retirement table.

## What expires, and what does not

A delivery in the explicitly retired interval rejects with
`ERR_FSQLITE_ORDER_EXPIRED`. Its application callback does not run; no receipt is
invented and no successful replay ACK is returned. Retained anchors/recent
receipts and protected seed receipts still check the exact identity and digest
and return their original decisions without applying SQL again.

**Retiring ends exact replay and delivery-ID reuse detection for the removed
identities.** Never reuse a delivery ID for different work or change an old
message's source sequence to bypass expiry. A buggy or malicious producer that
reassigns a retired ID to a new sequence is outside this retention contract.
Retire only after the source has durably acknowledged those deliveries and the
required retry/redelivery horizon has ended. An expired request is not permission
to acknowledge or discard pending source data. This is not automated source
watermark agreement, consensus, transport authentication or an exactly-once
promise extending beyond the retained evidence.

`maxEntries` includes protected bootstrap receipts, the retained anchor and all
newer identities, but not genesis. Reserve at least two entries beyond the seed
prefix for recurring apply/retire progress. Retirement never deletes the seed to
make room: a seed using the complete configured capacity still needs headroom.
Source outbox, ordinary inbox and rebase-journal retention remain their own APIs;
this method changes only the order ledger and its retirement anchor.

## Atomicity, compatibility and integrity

The metadata in reserved `main.__fsqlite_changeset_order_retention` is validated
as an ordinary two-column table, with no triggers, additional indexes or foreign
keys. Its canonical versioned record binds the original source/receiver, the
protected seed extent, and an exact retained boundary receipt. Identity JSON
and UTF-16 storage expansion remain bounded; public identity limits are unchanged.

Retiring receipts are validated in primary-key pages of at most 32 entries before
any deletion. Anchor publication and range deletion use the same owned SQL
transaction. Incorrect write counts, SQL errors, cancellation, deadlines and a
failed COMMIT roll both back. A lost response after COMMIT can mean retirement
succeeded: reopen/retry the same or older boundary, never restore or manufacture
old receipts. Nested targets remain provisional until their enclosing transaction
commits. The operation itself is SQL-only and supplies no storage confirmation.

Ledger reads validate exact populations and the two allowed sequence intervals,
plus genesis, head, boundary receipt and bootstrap protection. Missing live
receipts, a missing anchor/table, unexpected receipts in a retired interval or
changed metadata fail rather than being silently repaired. An orphan retirement
record cannot be used to initialize a new stream. Application callbacks cannot
change retirement while advancing a delivery. Independent connections rely on
the target's transaction/conflict semantics; no global writer lock or retry loop
is introduced. One instance rejects overlap with an admitted apply/retirement.

Legacy ledgers need no migration and retain their behavior until explicit
retirement. Older SDK versions that do not understand retirement fail closed on
the resulting sequence gaps; do not run them against a retired ledger. Database
metadata and SQL adapters remain trusted. Whole-database backups restore their
own older head/retirement history, and a writer able to forge all local metadata
is not constrained by a local receipt hash. No external rollback detector is added.

Metadata collection is bounded, not total database size, SQL scan work or process
RSS. Population checks scan the retained ledger as before; no latency or throughput
improvement is claimed. Application rows and their indexes are untouched.

## Executed verification

```sh
node --experimental-transform-types \
  --experimental-loader=./packages/sdk/tests/helpers/production-source-loader.mjs \
  --test packages/sdk/tests/changeset-order-retention.test.mjs \
  packages/sdk/tests/changeset-bootstrap-production.test.mjs
```

On Node 22.16.0 / reference SQLite 3.49.1, 89 tests pass without skips: 52 new
retirement cases and the 37 production-bootstrap integration cases. Tests run
actual SDK order, bootstrap, outbox, transfer, HTTP, application and codec modules,
not substituted replication implementations. The SQL/transaction owner is a
reference SQLite adapter. Strict TypeScript 5.8.3 checks use actual dependencies.

Coverage includes a full ledger resuming after retirement; 120 deliveries with
a four-entry cap; all three SQLite encodings; maximum JSON-escaped identities;
protected bootstrap replay after later increments; gaps, altered anchors and
corrupt interior metadata; bounded indexed pages; cancellation and timeout;
outer/deferred-COMMIT rollback; a lost COMMIT response; callback interference;
and overlapping file-backed retirement owners. Eight new child processes are
actually SIGKILLed at anchor publication, deletion and before/after COMMIT across
WAL and DELETE journals. Fresh connections recover all-or-none retirement and
continue with the next sequence. The bootstrap suite adds 24 earlier process cuts.

This is SDK-module behavior on reference SQLite, not FrankenSQLite Rust/WASM/MVCC,
full SDK/worker packaging, browser persistence, production TLS or physical
power-loss qualification. No native concurrency defaults or workflows change.
