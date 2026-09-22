# Durable all-replica changeset fanout

`ChangesetFanout` lets several fixed replicas consume one `ChangesetOutbox`
without copying the source payload per recipient. Each replica advances an
independent SQL-backed cursor. A fast replica cannot reclaim a payload still
needed by another member. Reclamation happens atomically with the last required
acknowledgement, not with the first successful network delivery.

```ts
import { ChangesetFanout, ChangesetOutbox, ChangesetDeliveryPump } from "@frankensqlite/sdk";

// source implements ChangesetTarget (an exclusively owned FrankenDB or queue).
// Set recursive_triggers=ON before recording source DML. Initialize fanout
// BEFORE the outbox's first bootstrap or incremental operation.
const fanout = await ChangesetFanout.open(source, ["east", "west"]);
const outbox = new ChangesetOutbox(source);
await outbox.record(
  tx => tx.execute("INSERT INTO notes(id, body) VALUES (?,?)", [1n, "hello"]),
  { deliveryId: "device-123:operation-1", tables: ["notes"] },
);

const east = new ChangesetDeliveryPump(fanout.forReplica("east"), {
  receiverId: "east", deliver: deliverToEast, confirmSource,
});
const west = new ChangesetDeliveryPump(fanout.forReplica("west"), {
  receiverId: "west", deliver: deliverToWest, confirmSource,
});
// No source SQL transaction stays open during transport. Use the existing
// target ownership/queue policy; retry the same source-qualified identities.
await east.run();
await west.run();
console.log(await fanout.progress());
```

Supply authenticated transports and a receiver that confirms the exact delivery
ID, digest, counts, and receiver identity. `receiverId` is routing, not
authentication. Direct `acknowledge()` is only appropriate after a matching
durable receiver ACK. The receiver-bound adapter supports the existing pump's
`pending`, `read`, and `acknowledge` interface. Match each pump's receiverId to its
adapter; do not route a member's acknowledgements to another member.

## Persistence and recovery

Membership and cursors are persisted in `__fsqlite_changeset_fanout` and
`__fsqlite_changeset_fanout_progress` in the same source database as the outbox.
Opening with the same set of case-sensitive IDs resumes progress; input order
is irrelevant. There must be 1..256 distinct identities, at most 256 UTF-8 bytes
each. Membership is immutable. Adding/removing members, replacing a replica with
an empty database under its old identity, quorum reclamation, and automatic
expiry are not supported. Bootstrap a new group explicitly instead.

Missing members, partial schemas, forged cursors, skipped source rows, or
prematurely reclaimed payloads stop delivery rather than silently shrinking the
group. A member must acknowledge its immediate next operation. Repeating an
exact retained acknowledgement is idempotent. SQL commit failure rolls back
cursor advancement and reclamation together.

SQL acknowledgement is not an unconditional disk-durability receipt. Snapshot
persistence still requires the same source's awaited `confirmSource` checkpoint/
recovery barrier and the receiver's `confirmCommit` barrier. The existing pump
provides those boundaries, including retries. Memory targets and no-op
confirmation do not establish persistence. This API does not implement native
cross-process MVCC, transport authentication, or automatic crash-proof browser
page persistence. Do not externally edit SDK metadata or treat source callbacks
as an SQL security sandbox.

`ChangesetOutbox.record`, `bootstrap`, and `bootstrapChunks` remain the source
publication APIs. Their callbacks cannot commit edits to fanout membership or
progress. Ordinary outbox `acknowledge` and forgetting methods reject while
fanout is configured so they cannot bypass a required recipient. Outboxes
without fanout retain their existing single-recipient behavior.

## Bootstrap and retention

Single and chunked bootstrap may follow initialization on a pristine outbox.
Every replica independently consumes the complete seed before live changes.
The existing bootstrap receiver's complete-install/confirmation boundary still
controls publication; per-chunk acknowledgements do not publish an application
baseline by themselves.

`progress()` reports `sourceSequence`, `acknowledgedThrough` (the minimum required
cursor), and frozen per-replica receipt identities. All sequences remain bigint.
Per-replica `pending()` is bounded and numerically ordered. `read()` returns
owned, digest-verified bytes.

Payload bytes are reclaimed at the all-member frontier. Identities never expire
automatically. Call `fanout.forgetAcknowledged(deliveryId, sha256)` only when all
members have advanced strictly beyond the identity. Current cursor receipts must
remain retained for validation. Use `fanout.forgetBootstrapChunks(rootId, sha256)`
for whole-seed cleanup after all members advance beyond the complete seed; it
never removes a partial manifest. Forgetting ends ID deduplication. A slow or
offline member may fill the bounded outbox: backpressure is intentional, never
silent data loss.

## Reference-SQLite verification

From the repository root with Node 22.16+ (no npm install or generated WASM):

```sh
node --experimental-transform-types \
  --experimental-loader=./packages/sdk/tests/helpers/fanout-source-loader.mjs \
  --test packages/sdk/tests/changeset-fanout.test.mjs
```

Tests execute production TypeScript fanout/storage/codec code, real SQLite
session changesets, native SQLite application, file reopen and SQL rollback.
They do not execute the FrankenSQLite Rust/WASM engine, browser IndexedDB
durability, or multi-process native MVCC.
