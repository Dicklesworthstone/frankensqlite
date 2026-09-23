# Atomic receiver-side bootstrap installation

`ChangesetBootstrapReceiver` closes the visibility gap in per-message seed
application. Uploads retain binary chunks in reserved SQL staging tables without
inserting application rows. `install` revalidates the complete staged baseline,
applies every chunk and records the installation decision in ONE transaction,
then awaits the configured storage-confirmation boundary. A failure in any chunk
or at COMMIT rolls back all application rows and leaves the staged bytes for
recovery. No SQL transaction remains open while waiting for another upload.

```ts
import { createBootstrapManifest, ChangesetBootstrapReceiver } from '@frankensqlite/sdk';

// readChunk reads immutable, already-retained source bytes, not a fresh query
// against changing source tables. Totals come from the completed source seed.
const manifest = await createBootstrapManifest({
  receiverId: 'replica-42', deliveryId: 'source-42:baseline',
  tables: ['parents', 'children'], chunks, changes, byteLength,
}, readChunk);
const receiver = new ChangesetBootstrapReceiver(destination, {
  receiverId: 'replica-42', tables: ['parents', 'children'],
  confirmCommit: () => destination.checkpoint(), // Same top-level snapshot DB.
});
for (let index = 0; index < manifest.chunks; index++) {
  await receiver.stage(manifest, index, await readChunk(index));
}
const receipt = await receiver.install(manifest);
// Only this confirmed installation receipt can authorize source reclamation.
```

## Identity, integrity and authority

This is a separate `fsqlite-bootstrap-v1` protocol. A staging result is NOT an
ordinary changeset delivery receipt and cannot be passed to the existing pump as
an application acknowledgement. Uploads must be authenticated by the host before
calling this API. Routing IDs and SHA-256 hashes are not credentials. The fixed
receiver allowlist is checked on every operation, including installed replays.
Only INSERT changes for manifest tables are accepted. Receiver conflict omissions
and destructive replacement of existing rows are deliberately unsupported.

The manifest's ordered hash binds recipient, source-qualified root identity,
table scope, chunk count, row count, byte count, and every indexed chunk digest.
For interoperable construction, H0 is SHA-256 of UTF-8 JSON.stringify of:
`["fsqlite-bootstrap-v1", receiverId, deliveryId, tables, chunks, changes, byteLength]`.
H(i+1) hashes `["fsqlite-bootstrap-v1", H(i), i, chunkSHA256, chunkBytes, chunkChanges]`.
`sha256` is H(N), not the first chunk's digest. Digests are lowercase hexadecimal;
numbers are bounded safe integers; table names are ASCII-case-folded, distinct,
and remain in the supplied order. UTF-8 names and IDs reject NUL and unpaired
surrogates. `createBootstrapManifest` independently validates bytes and totals,
retaining one chunk at a time. It does not establish a new source snapshot.

Each receiver database supports one retained bootstrap identity for its lifetime.
Different roots, recipient IDs, scopes or digests reject, even after installation.
There is no automatic expiry, reseed, reset or overwrite. An empty source still
has one empty chunk and an installation decision. Do not modify the reserved
`__fsqlite_bootstrap_state` and `__fsqlite_bootstrap_chunks` tables. Their schemas,
indexes and trigger/foreign-key absence are checked before use. A restored backup
restores its own older receipt state; this protocol cannot infer later history.

## Staging, recovery and SQL visibility

`stage(manifest, index, bytes)` requires a contiguous zero-based upload prefix.
Repeating an existing index with the same digest/length/count returns progress
without appending data. Changed duplicates, missing predecessors, unauthorized
rows and an incorrect final manifest hash reject. Upload progress and bytes are
stored transactionally. A process can reopen and continue uploading; no live
source cursor or network transaction is retained.

`status` reads that progress without creating storage tables or confirming them.
Its `installed` flag reports SQL state, not a storage-confirmed ACK. Staging is
not checkpointed automatically: a receiver crash may lose volatile upload progress,
so the source MUST retain all payloads until final installation confirmation.

All selected destination tables must already exist and be empty at installation,
including selected tables with zero source rows. They must be ordinary main tables
with visible, nongenerated columns and declared keys. Application triggers on
selected tables reject. Foreign-key and other constraints retain their ordinary
semantics; no PRAGMA is changed. Parent-first chunk order handles immediate FKs;
explicitly deferred constraints are checked by the final transaction COMMIT.
Default expressions, the adapter and local database schema remain trusted.

Installation reads and verifies one chunk at a time and calls the existing
`applyChangeset` row implementation within its outer transaction. It creates no
per-chunk inbox receipts and permits no omissions. Raw staged payloads are cleared
in that SAME transaction as the rows and installed flag; chunk metadata remains
for replay validation. SQL readers using the target's transactional isolation see
no committed partial baseline. This is an all-or-none row install into empty
existing tables, not a schema clone or an atomic swap of an existing database.

After a lost COMMIT response, failed checkpoint or lost final ACK, call `install`
with the SAME manifest. An installed receipt skips all row SQL but runs storage
confirmation again. Later legitimate application changes do not cause a baseline
replay. A SQL-installed baseline may be visible while confirmation is pending;
applications requiring durable publication must also await final confirmation.
Never use an enclosing transaction/savepoint as the receiver target: its later
rollback would invalidate an already issued ACK. The trusted target and confirmation
callback must refer to the same TOP-LEVEL database. Durable SQL targets can explicitly
supply an async no-op only when their own commit contract already supplies durability.
A no-op on an in-memory database does not make it persistent.

## Atomic handoff to ordered incremental replication

For a seed created as the first `ChangesetOutbox.bootstrapChunks` operation,
configure `orderedSourceId` **before staging chunk zero**. Use the same trusted
source incarnation as the incremental ordered transport. The manifest's root
delivery identity and exact chunk bytes must be those retained by that source
outbox, not a newly generated snapshot of later data.

```ts
import {
  ChangesetBootstrapReceiver, ChangesetOrder,
  createOrderedChangesetReceiver, applyChangeset,
} from '@frankensqlite/sdk';

const seedReceiver = new ChangesetBootstrapReceiver(destination, {
  receiverId: 'replica-42', tables: ['notes'],
  orderedSourceId: 'source-42:incarnation-1',
  confirmCommit: confirmDestinationCommit,
});
// Stage the immutable manifest/chunks as above, then:
const installed = await seedReceiver.install(manifest);
console.log(installed.order?.sequence); // N seed chunks occupy sequences 1..N.

const order = new ChangesetOrder(destination, {
  receiverId: 'replica-42', sourceId: 'source-42:incarnation-1',
});
// No separate initialize/reset is needed after installation.
const incremental = await createOrderedChangesetReceiver(order, {
  apply: (inside, message, controls) => applyChangeset(inside, message.changeset, {
    tables: ['notes'], deliveryId: message.deliveryId, ...controls,
  }),
  confirmCommit: confirmDestinationCommit,
});
// The next source outbox message must have its original sequence N+1.
```

All baseline rows, the existing order ledger's per-chunk receipts and head,
staging-body reclamation, and the installed marker commit in **one** transaction.
There is no interval between committed baseline installation and order enrollment.
Staging alone does not initialize an order ledger. A matching, empty genesis may
already exist; a nonempty or differently bound ledger refuses installation and
rolls back all its row writes. Nothing is renumbered or silently rebased.

Once an ordered manifest is staged, `ChangesetOrder.apply` refuses its stream
until installation commits, even when an incremental endpoint was already open
on an initialized genesis. A pump therefore cannot publish the seed one chunk at
a time through the incremental API. Application callbacks cannot remove or
change the bootstrap authority while advancing the order ledger. Each ordinary
delivery checks the installed binding and terminal prefix receipt; full retained
seed-prefix verification remains part of bootstrap replay, not every increment.

Seed sequence `i+1` retains the original root ID at `i=0`, otherwise
`root/chunk/i`, together with its verified byte digest, length and applied-row
count. These are replay records, not separately committed chunk applications.
Historical ordered seed retries return their original decisions without inserting
rows again. An empty seed still occupies its original source sequence.

The installed receipt adds a frozen, JSON-serializable `order` field containing
`protocol: 'fsqlite-ordered-changeset-v1'`, `streamId`, and the canonical decimal
`sequence`. It identifies the **seed's** final sequence, not a later incremental
tip. It is still a bootstrap receipt, not a per-delivery ACK. Authenticate the
transport and verify the exact manifest/receiver before source reclamation; this
option does not acknowledge source entries or change fanout membership.

Reopen with the same `orderedSourceId`. It is stored as local policy alongside the
manifest, independently of the portable wire hash. Changing it, omitting it, or
adding it to an existing unordered bootstrap is refused. Installed replay checks
the retained chunk metadata against the manifest hash and every seed ledger
receipt, without needing reclaimed payloads or rerunning application SQL. It
never recreates missing order tables/receipts or rewinds newer increments. A failed
check prevents confirmation. Whole-database restoration can still restore older
history; hashes are not authentication or an external rollback detector.

The prefix writer retains one metadata entry at a time and updates the order head
once. The existing 100,000-entry order limit includes seed chunks; a seed that
uses the entire limit leaves no room for incremental entries. Reserve headroom.
Foreign-key rules remain unchanged; explicitly deferred constraints or a suitable
whole-install transaction policy are required for cross-chunk cyclic dependencies.

The cross-component regression suite runs the production bootstrap, codec,
`applyChangeset`, and order ledger together on reference SQLite, including native
Session-generated seeds followed by actual SDK incremental application:

```sh
node --experimental-transform-types \
  --experimental-loader=./packages/sdk/tests/helpers/fanout-source-loader.mjs \
  --test packages/sdk/tests/changeset-bootstrap-order.test.mjs
```

Process-death coverage kills a separate Node process at row application, prefix
insertion, head publication, body reclamation, before/after COMMIT and confirmation,
under both WAL and DELETE journals. Fresh SQLite connections verify all-or-none
baseline/order publication and retry the same retained manifest.

This is not validation of the FrankenSQLite Rust/WASM engine, the complete HTTP
and source-pump path, browser snapshot persistence, or physical power loss.

## Acknowledging the complete seed at the source

For a single-recipient `ChangesetOutbox.bootstrapChunks` source, use
`acknowledgeBootstrapInstall` after receiving an authenticated, confirmed install
receipt. This avoids retransmitting every installed seed chunk just to obtain
individual replay acknowledgements before incremental delivery can start.

```ts
import { acknowledgeBootstrapInstall } from '@frankensqlite/sdk';

// originalManifest is the exact manifest sent to this trusted receiver.
// Do not construct it from fields of the incoming receipt.
const newlyAcknowledged = await acknowledgeBootstrapInstall(
  source, originalManifest, installedReceipt, {
    receiverId: 'replica-42',
    orderedSourceId: 'source-42:incarnation-1',
    signal: cancellationSignal,
  },
);
await confirmSourceCommit(); // Required for a snapshot-backed source, even on replay.
// The ordinary source pump can now select the original increment at N+1.
```

The helper verifies the receiver, root identity, complete manifest hash, totals,
and confirmed installation decision before entering source SQL. Ordered receipts
also require the configured source incarnation and exact seed frontier. Configure
`receiverId` and `orderedSourceId` from trusted routing state, never incoming ACK
fields. An unordered install omits `orderedSourceId`; an ordered receipt cannot
be silently accepted through that policy. Stage/status results are not install
receipts and cannot authorize reclamation. Hash matching is not authentication.

Within one source transaction, the helper validates the outbox schema, full
contiguous seed scope and metadata, every still-pending payload, and the complete
receiver-specific manifest hash chain. Only then does one UPDATE acknowledge all
remaining seed entries and clear their payloads. Later incremental entries,
original identity tombstones, and the AUTOINCREMENT sequence remain unchanged.
Metadata is paged and bodies are verified one at a time, not collected into a
whole-baseline array; this is not an RSS or SQL-engine allocation bound.

The return value is the number of newly acknowledged chunks. A partial ordinary
acknowledgement prefix is completed safely; an exact retained replay returns zero
without requiring already reclaimed bodies. A missing/forgotten seed is an error,
not an empty successful acknowledgement. After a lost source COMMIT response,
reopen/reconcile and retry the same manifest and receipt. No source DML or receiver
application is repeated. Failure before source COMMIT rolls back all reclamation;
a response lost after COMMIT cannot be interpreted as rollback.

This helper does not contact the receiver, checkpoint either database, authenticate
the peer, forget tombstones, or update fanout progress. It rejects a source with
fanout membership (including a partial/corrupt membership schema): one receiver
cannot authorize deleting data still needed by another. Such sources use
`acknowledgeFanoutBootstrapInstall` below or their receiver-bound per-message
acknowledgement path. A source transaction nested in an
outer transaction remains provisional until that outer transaction commits.

The source regression suite executes the real SDK capture, outbox, manifest,
bootstrap installer, row application, order ledger, and source acknowledgement
against reference SQLite transaction owners. It includes forged/missing ACKs,
source corruption, partial acknowledgements, preserved incremental bytes,
cancellation, deferred-COMMIT failures, reopen, and six process-kill/reopen cases
around reclamation and COMMIT under WAL and DELETE journals:

```sh
node --experimental-transform-types \
  --experimental-loader=./packages/sdk/tests/helpers/fanout-source-loader.mjs \
  --test packages/sdk/tests/changeset-bootstrap-ack.test.mjs
```

These are not executions of the FrankenSQLite Rust/WASM engine, browser snapshot
storage, the complete HTTP/pump chain, or physical power-loss tests.

## Complete installation acknowledgements for a fanout source

`acknowledgeFanoutBootstrapInstall(source, originalManifest, receipt, options)`
accepts one member's confirmed installation without acknowledging or retransmitting
every seed chunk separately. Initialize the immutable `ChangesetFanout` roster
before creating the source outbox's first seed, as usual. Build and retain each
receiver's own outbound manifest: its hash is recipient-specific even though all
members share the same source chunk bytes.

```ts
import { acknowledgeFanoutBootstrapInstall } from '@frankensqlite/sdk';

// eastManifest is the ORIGINAL manifest sent to this trusted receiver route.
// eastReceipt comes from its authenticated, completed install operation.
const advanced = await acknowledgeFanoutBootstrapInstall(
  source, eastManifest, eastReceipt,
  { receiverId: 'east', orderedSourceId: 'device-42:incarnation-7' },
);
await confirmSourceCommit(); // The SAME source; also required after an ACK replay.

// A fast member can now consume N+1 while other members still need seed chunks.
console.log(advanced, await fanout.progress());
await eastPump.run();
```

This uses the same strict receipt, route, full-source-manifest and pending-payload
verification as the single-recipient helper. The stored fanout roster and every
member cursor are validated in that same transaction. A receipt for a nonmember,
a missing cursor, a damaged roster, or source sequence holes reject. Neither API
falls back to the other when its required source state is absent or corrupt.
Use trusted routing configuration for `receiverId` and `orderedSourceId`; the
receipt is not permission to choose a different member or stream incarnation.

Only the named member advances to the seed's final original sequence N, with the
last seed chunk's retained identity and digest. Other member cursors and later
incremental entries remain unchanged. Payload reclamation advances only to the
minimum of ALL required member cursors, which may stop partway through the seed.
The cursor update and any newly permitted range reclamation commit together.
An offline member therefore continues to retain its required source payloads;
this is all-member retention, not a quorum, expiry, eviction or membership change.

The return value counts seed sequences newly acknowledged by THAT member. It is
not the number of globally reclaimed rows or bytes: the first member may return
N while reclaiming nothing. Already acknowledged per-message prefixes are handled
without replaying SQL. Exact install-ACK retries return zero, including after that
member has consumed newer increments, and never rewind its cursor. Retries still
verify the complete retained seed metadata and all globally pending seed bodies.
Forgetting a complete seed explicitly ends this replay guarantee; the helper
rejects missing history rather than silently reconstructing it.

No transport, source checkpoint, transaction retry loop or global writer mutex is
introduced. Concurrent connections use the source engine's ordinary transaction
conflict rules; reconcile and retry the same manifest and receipt on conflict or
a lost commit response. Cancellation drains started SQL before rejecting, and a
failed transaction rolls back both cursor advancement and reclamation. An outer
transaction still owns the final commit of a nested invocation. A response lost
after commit is not proof of rollback.

The fanout regression suite executes production capture, outbox, bootstrap,
`applyChangeset`, order and fanout code with reference SQLite transaction owners.
It checks fast/slow replicas through N+1, all 27 three-member combinations of
0/1/2 seed acknowledgements, multi-page seeds, empty seeds, historical retries,
receipt forgery, corrupt progress/payloads, cancellation, false affected-row
results, deferred commit failure, two overlapping file-backed source owners and
eight SIGKILL/reopen boundaries under WAL
and DELETE journals. Its row oracle also applies the same changesets through
native SQLite Session APIs. These tests do not qualify the FrankenSQLite
Rust/WASM engine, browser durability, the full HTTP/pump chain or physical power
loss.

```sh
node --experimental-transform-types \
  --experimental-loader=./packages/sdk/tests/helpers/fanout-source-loader.mjs \
  --test packages/sdk/tests/changeset-fanout-bootstrap.test.mjs
```

## Resource and cancellation contract

Default limits are 8 MiB per chunk, 256 MiB total wire bytes, 10,000 chunks and
1,000,000 rows. Configurable hard limits are 64 MiB, 1 GiB, 100,000 chunks and
10,000,000 rows. Codec limits apply per chunk. Metadata is bounded; payload length
is checked in SQL before loading. One payload and its decoded rows are materialized
at a time, with bounded hashing/copying overhead. This is not an RSS, SQL write-buffer,
WAL-size or transaction-duration guarantee: final installation is a large transaction.

Only one operation is admitted per receiver instance; excess calls reject without
queueing or copying another upload. Independent connections rely on the database's
existing conflict engine, not a new global lock. Cancellation and monotonic deadlines
check between SQL operations and propagate into changeset application. Started
SQL and confirmation are awaited, not abandoned. Cancellation during a successful
COMMIT still drains confirmation before rejecting. A trusted blocking adapter or
confirmation callback can delay settlement. Errors after commit do not prove rollback.

## Executed evidence

61 tests passed on Node 22.16.0 / SQLite 3.49.1, with zero failures or skips.
Strict TypeScript 5.8.3 checks passed against the actual bootstrap, application
and codec sources (including exact optional properties and unchecked indexes).
The suite checks stage invisibility, cross-table observer isolation, late-chunk
and deferred-COMMIT rollback, restored upload progress, lost responses, confirmation
failure, duplicate identity, scalar preservation, quotas and damaged stage data.
Twelve native-session baselines converge through subsequent incremental application.
Two real file connections exercise overlapping staging and installation.

Child processes are SIGKILLed midway through application, before COMMIT, after
COMMIT and during confirmation. Fresh connections find all-or-none application rows,
recover staged bytes or the installed decision, and complete without duplicate inserts.
These are process-death tests on the reference engine, not power-loss certification.

```sh
node --experimental-loader=./packages/sdk/tests/helpers/source-loader.mjs \
  --test packages/sdk/tests/changeset-bootstrap.test.mjs
```

Full SDK/worker builds, FrankenSQLite Rust/WASM execution, browser persistence,
production transport deployment, physical power loss and native RaptorQ replication
are not certified by these tests. No workflow or concurrent-writer defaults change.
