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

## Retire acknowledged original edits

`journal.retireLocal(operationId, recordSha256, controls?)` explicitly removes
one retained ORIGINAL after its exact rebased publication is acknowledged. This
releases `maxLocalEntries` and `maxLocalBytes` capacity; acknowledging the outgoing
message alone does not release the original. Nothing expires automatically.

```ts
// First complete delivery and source/receiver confirmation with the normal pump.
// Use the original recordSha256 returned by captureLocal or enqueueLocal, NOT
// the outgoing delivery.sha256 (rebasing can change those bytes).
const retired = await journal.retireLocal('edit-108', published.recordSha256);
// Snapshot-backed sources still need confirmation of this same database.
await source.checkpoint();
console.log(retired.removed, retired.byteLength);
```

The frozen result contains `removed`, `byteLength`, `recordSha256`, and the
acknowledged `delivery`. `byteLength` counts original wire bytes removed in THIS
call, not disk space reclaimed. A retry finding no original returns false/zero,
but must still find the matching, acknowledged, sealed publication. It does not
infer why the original disappeared. A missing locals table or publication rejects
rather than reconstructing history or treating an unknown operation as completed.

Retirement verifies the original payload and record checksum, original scope and
basis, complete canonical publication seal, output identity/digest/counters, and
reclaimed output shape. When fanout exists, its whole roster, required cursors,
and reclamation frontier are validated in the same transaction. A single faster
replica, empty rebased payload, or merely present publication cannot authorize
removal. Missing, unacknowledged, mismatched, or corrupted evidence rejects.

The deletion is conditional on the exact original identity and record checksum;
its affected-row count, resulting local accounting, surviving publication, and
replica state are checked before commit. SQL errors, cancellation, deadlines,
failed commit, and outer rollback preserve the original. An admitted SQL call
settles before cancellation propagates. Two connections use the transaction
owner's normal conflict semantics, with no global lock, waiting queue, or retry.
A lost response after COMMIT can mean removal succeeded: reopen and reconcile the
same operation and checksum. A nested result is provisional until the outer commit.

Application rows, source sequence, outbox publication, remote decisions, inbox
receipts, and replica cursors are not removed or rewritten. Cleanup neither
rescans application tables nor requires their old schema or remote history to be
available. It can therefore release a delivered original after later application
changes without reapplying its SQL. It does not repair damaged remote history.

After retirement, `readLocal` returns null and `rebaseLocal`/`enqueueLocal` reject
because the original no longer exists. `captureLocal` rejects before calling
business code whenever the derived publication survives without an original.
This also prevents recapture when an original was accidentally lost while its
publication is still pending. The publication ID check runs before and after
new capture work. Existing original replays are unchanged.

Keep the outbox identity through the required application retry horizon.
Separately forgetting that identity ends this recapture guard; NEVER reuse an
operation ID for new work afterward. SDK versions without this guard must not
write to a database after originals have been retired: they cannot distinguish
removed original records from new operations. No automatic cross-version fencing
or external rollback detector is provided. The SQL owner and local metadata are
trusted. An acknowledged flag/cursor is not independently authenticated proof of
remote durability; the existing delivery/confirmation contract must be honored.

Retirement checks one original at a time under the journal's codec limits. It
ignores aggregate local capacity ceilings so an over-cap database can be reduced,
but validates aggregate accounting. Count/byte scans and fanout validation retain
their costs; original buffers, decoded values and hash copies can coexist. This
bounds logical data, not engine memory/RSS or cleanup latency. Outbox identities
and remote conflict history retain their separate limits and policies.

### Retirement verification

```sh
node --experimental-transform-types \
  --experimental-loader=./packages/sdk/tests/helpers/production-source-loader.mjs \
  --test packages/sdk/tests/changeset-local-retirement.test.mjs
```

62 tests pass on Node 22.16.0 / reference SQLite 3.49.1 with no skips. The actual
journal, capture, rebaser, application, codec, FK helper, outbox store, and fanout
modules run without replacements. Single-recipient tests use the real store's
acknowledgement function; required-replica tests use the public fanout API. The
40-operation capacity test applies the actual outgoing changes to a second SQL
owner, acknowledges them, retires originals under a one-entry local cap, and
explicitly forgets outbox identities. Other tests cover all three encodings,
native Session byte equivalence, corruption, slow replicas, empty output, lowered
caps, changed schemas/history, rollback, delayed cancellation, and lost responses.

Eight child processes emit an IPC cut marker and are actually SIGKILLed before/
after deletion and before/after COMMIT under WAL/DELETE journals. Watchdog kills
fail rather than count as recovery evidence. Independent WAL owners also overlap
before their competing deletes. Strict TypeScript 5.8.3 checks all eight actual
transitive source modules, without declaration substitutes. These are targeted
retirement tests, not a rerun of the earlier 67-test publication suite, HTTP,
full SDK/worker, FrankenSQLite Rust/WASM/MVCC, browser storage, or power-loss proof.
