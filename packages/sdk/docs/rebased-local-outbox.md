# Atomically publish rebased local operations

`ChangesetRebaseJournal.enqueueLocal(operationId, options)` connects retained
original local changes to the existing source outbox. It verifies the original
record and its saved history basis, rebases over a selected complete remote
history prefix, and retains the resulting payload and that choice in ONE SQL
transaction. It never reapplies local business SQL or replaces the original.

```ts
const journal = new ChangesetRebaseJournal(source, {
  journalId: 'source-42:conflict-history',
});
// The original was recorded with the local SQL in an earlier transaction:
await journal.captureLocal('edit-108', tx => tx.execute(
  'UPDATE notes SET body=? WHERE id=?', ['local revision', 12n],
), { tables: ['notes'] });
// Receive remote changes through journal.apply() (or a journaled receiver).
// At the application's chosen publication boundary:
const published = await journal.enqueueLocal('edit-108', {
  tables: ['notes'],
  through: await journal.bookmark(),
  maxEntries: 10_000,
  maxPayloadBytes: 64 * 1024 * 1024,
});
const outbox = new ChangesetOutbox(source);
const pending = await outbox.read(published.delivery.deliveryId);
// Use the existing delivery pump/transport to deliver pending work and confirm
// source and receiver storage. Publication alone is not delivery or durability.
```

`tables` must be the original capture's complete table scope, including empty
selected tables. Order and ASCII case differences are normalized. The original
scope's indirect policy is recovered, not overridden. Even a net-zero original
or a fully superseded local edit publishes a valid empty delivery, preserving
source sequence and acknowledgement semantics.

`through` may be a journal position or a verified bookmark, as for `rebaseLocal`.
A bookmark is preferable when publication must use a previously selected prefix.
Omitting it chooses the transaction snapshot's tip on the FIRST publication only.
The original's saved basis is always used; callers cannot supply alternate bytes
or an `after` boundary. Missing, corrupt or replaced history rejects new work.
The original, remote history, application rows and incoming receipts are not
modified by this method.

## Identity and recovery

The delivery ID is deterministically derived as `fsqlite-rebase-outbox-v1:` plus
SHA-256 of the UTF-8 JSON array `[journalId, operationId]`. Use a stable, globally
source-qualified journal ID. The derived ID is opaque; hashing is not transport
authentication. Configure endpoint/caller authorization for these identities,
not an assumption that the original source name appears in the wire ID.

One retained original therefore selects one retained outgoing delivery, not a
new ID every time its history changes. A repeated call validates and returns the
original publication with `replayed: true`, including after acknowledgement.
An explicit retry boundary must match the retained one; an implicit retry does
not select the newer tip. It does not rebase again or require newer remote
history. It still verifies the retained original record and pending payload.
Acknowledged payloads stay reclaimed; no bytes are regenerated.

The outbox scope contains the original record digest, both history bookmarks,
table/indirect policy, and a seal binding that evidence to the delivery identity,
payload hash, length and change count. Malformed or altered evidence rejects.
The ordinary outbox reader also verifies pending payloads. These checks detect
inconsistent local metadata, not an authorized writer forging all records or
restoring a whole older database. No anti-rollback service is implied.

A lost response after COMMIT can mean publication succeeded. Reopen and retry
the SAME journal and operation ID. Before COMMIT, failure rolls back the whole
publication. Enclosing-transaction results remain provisional until their outer
commit. Snapshot-backed storage still needs a genuine same-database checkpoint
and recovery barrier before transport. The method does not checkpoint, send,
acknowledge, retry automatically, or release original/history records.

Explicitly forgetting the outbox identity ends publication replay protection.
Do not enqueue that original again after forgetting or recycle its operation ID.
The original remains available for local recovery, not as permission to resend
already-delivered work under another identity.

## Ordering, replicas and budgets

Enqueue local operations in their original application order and choose history
boundaries appropriate for the receiver. Independent calls are not a local
operation scheduler, and this method does not prove remote history agreement or
that an arbitrary source/receiver state is compatible. Do not put both an original
unrebased message and its rebased version into the same delivery stream. Receiver
conflicts remain real errors or explicit application decisions.

Published messages use the existing outbox sequences and wire format. Provision
an initial baseline first when one is needed. Required-replica membership must be
configured before the first outbox publication. Its normal all-replica retention
preserves bytes for slower members; this API does not change membership, advance
replica cursors or bypass their acknowledgement frontier.

`maxEntries` and `maxPayloadBytes` have the ordinary outbox defaults/ceilings:
10,000 / 100,000 retained identities and 64 MiB / 1 GiB pending payload bytes.
A full identity budget rejects new work before loading originals or traversing
history. Original/output messages and the combined rebaser remain governed by
the journal's codec limits. A retained retry does not need new capacity.

History verification is linear in the selected prefix and retains one decision
at a time, plus combined rebaser state. Original/output buffers, decoded values,
hashing and validation copies can coexist. These are bounded logical-data limits,
not constant-time work, total SQLite memory or RSS guarantees. No native writer
lock or transaction retry is added. The supplied transaction owner must provide
isolation and roll back on failure; cancellation is checked at SQL boundaries.

## Executed verification

Run from the repository root on Node 22.16.0 or a compatible runtime:

```sh
node --experimental-transform-types \
  --experimental-loader=./packages/sdk/tests/helpers/production-source-loader.mjs \
  --test packages/sdk/tests/changeset-rebase-outbox.test.mjs
```

67 tests pass without failures or skips on reference SQLite 3.49.1. Actual
journal, capture, rebase, codec, application, outbox/store and fanout modules run
together; no replication backend is replaced. Tests include native Session
application, all three database encodings, large escaped identities, binary
keys/values, empty operations, pinned history, lost publication responses,
changed remote history, corruption, capacity, cancellation, nested/deferred
rollback, two required replicas and overlapping file-backed publishers.

Eight child processes are SIGKILLed before/after outbox insertion and before/after
COMMIT, under WAL and DELETE journals. Fresh connections recover all-or-none
publication without repeating original work. A watchdog kill fails the test; it
cannot count as reaching a requested cut. The first five regression cases fail
against the unmodified journal, which has no publication method.

Strict TypeScript 5.8.3 checks the actual transitive sources without declaration
substitutes. This is SDK-component evidence over reference SQL ownership, not
FrankenSQLite Rust/WASM/MVCC, full SDK/worker packaging, HTTP/TLS deployment,
browser persistence or physical power-loss qualification.
