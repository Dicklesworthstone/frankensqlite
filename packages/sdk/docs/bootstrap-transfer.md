# Recoverable delivery of an atomic bootstrap

`ChangesetBootstrapTransfer` connects the existing retained source manifest,
chunk store, atomic receiver and whole-install acknowledgement APIs. It is not
the per-message `ChangesetDeliveryPump`: uploading a seed chunk must never be
mistaken for applying it to application tables or authorizing source reclamation.

```ts
import {
  ChangesetBootstrapTransfer,
  ChangesetBootstrapReceiver,
} from '@frankensqlite/sdk';

// The source already retained its first ChangesetOutbox.bootstrapChunks seed.
// Destination tables are provisioned separately and must be compatible/empty.
const receiver = new ChangesetBootstrapReceiver(destination, {
  receiverId: 'replica-east',
  tables: ['parents', 'children'],
  orderedSourceId: 'source-42:incarnation-1',
  confirmCommit: () => destination.checkpoint(),
});
const transfer = new ChangesetBootstrapTransfer(source, {
  receiverId: 'replica-east',
  deliveryId: 'source-42:baseline-1',
  tables: ['parents', 'children'],
  orderedSourceId: 'source-42:incarnation-1',
  transport: receiver,
  confirmSource: () => source.checkpoint(),
});
const result = await transfer.run({
  maxChunks: 100,
  maxBytes: 64 * 1024 * 1024,
  timeoutMs: 30_000,
});
if (result.installed) {
  // Receiver install confirmation and the source ACK confirmation completed.
  // Existing ordered incremental delivery may now continue at sequence N+1.
  console.log(result.receipt, result.newlyAcknowledged);
}
// stopped:'limit' leaves a staged prefix and ALL source payloads retained.
// The application can await another bounded run; no background retry is started.
```

`transport` accepts the receiver directly, or authenticated asynchronous
`status(manifest, controls)`, `stage(manifest, index, bytes, controls)` and
`install(manifest, controls)` methods with the same contracts. No HTTP server,
wire framing, credential policy, listener or scheduler is created. The ordinary
HTTP changeset endpoint does not implement these separate bootstrap operations.
The host must route them to the correct receiver and authenticate responses.
Routing identities and hashes are not credentials.

Use the same TOP-LEVEL source and destination owners and their actual storage
confirmation functions. A nested transaction can roll back after an ACK and is
not a valid durability owner. Snapshot storage must checkpoint or recover at the
confirmation boundaries. An async no-op only makes sense for a SQL owner whose
commit already supplies the needed durability; it does not make memory durable.
The driver does not implement schema migration, replacement of existing rows,
source recapture or database-file swapping. Existing bootstrap schema, constraint
and immutable-history rules remain in force.

## Source proof, upload, installation and acknowledgement

Each run first confirms source state, even after an earlier ACK cleared every
body. An unresolved source publication must fail that callback instead of being
hidden by an empty pending list. The driver reads the original retained root;
missing/forgotten history is an error, never authority to create a new snapshot.

Before manifest recovery can transfer payloads into JavaScript, one scalar SQL
preflight checks the largest actual retained seed body against `maxChunkBytes`.
This check and `readBootstrapManifest` share ONE read-only source transaction.
That helper verifies pending payloads and reconstructs the original ordered
manifest from retained identities. Already acknowledged bodies need not exist.
No new source rows are captured. The table order in configuration must be the
original order: changing it changes the receiver-bound hash.

After confirming the source again, the driver queries receiver progress and
uploads from its next missing index. It loads and verifies one chunk at a time,
confirms the source after that read, awaits staging and checks returned counts.
All SQL transactions finish before calling transport or storage confirmation.
There is no durable local upload cursor that can skip receiver state lost after
a crash. A new run always obtains current receiver status. A missing source body
is accepted only when a fresh status reports the complete installed baseline;
then installation still has to be confirmed. Otherwise the run fails closed.

A complete staged prefix, or `status().installed`, is NOT an installation ACK.
The driver always calls `install`, including on replay. It checks and captures
the exact manifest identity, digest, totals, installed/confirmed flags, replay
flag and optional trusted ordered-source prefix before asynchronous source SQL.
Later mutation of the transport response cannot change the accepted receipt.

Only then does it call `acknowledgeBootstrapInstall`, or, when configured with
`acknowledgement: 'fanout'`, `acknowledgeFanoutBootstrapInstall`. The existing
source APIs revalidate retained evidence, preserve identity history, enforce
source-incarnation policy and prevent bypassing required replicas. The driver
never acknowledges individual seed chunks. A successful source ACK is followed
by source confirmation BEFORE success is returned, even when cancellation
arrives during that ACK. `newlyAcknowledged` counts advanced source seed
sequences; it is not a count of bytes reclaimed from a fanout source.

## Failures and bounds

A lost upload response is recovered through the next status. A lost installation
response is recovered by installed replay and renewed receiver confirmation.
An ACK may have committed even when its response or checkpoint was lost; the
next run first confirms source state and uses retained manifest metadata. It
never reruns a source business callback. A failed/unknown operation is not proof
of rollback. Never substitute a new root ID, discard installed history, or reset
sequence counters to bypass an error.

One run per instance is admitted; overlap rejects without allocating waiters.
Independent drivers may race. Matching receiver prefixes and installed replays
are supported, but the underlying transaction owners must supply their normal
isolation/conflict semantics. No global writer mutex or concurrency-default
change is introduced. Callback methods are captured once and bound to their
original object, including receivers with private fields.

`maxChunks` defaults to 100 and caps successful upload calls at 10,000 per run.
`maxBytes` defaults to 64 MiB and caps uploaded bytes at 1 GiB per run. If the
next body cannot fit after progress, the run returns `stopped: 'limit'`; a body
that cannot fit a fresh run rejects. No successor is sent out of order to fit a
budget. Finishing the last chunk permits installation within that same run.
`maxChunkBytes` defaults to 8 MiB and may be raised to 64 MiB. These are protocol
body budgets, not limits on engine memory, RSS, network buffering or snapshot
age. Manifest proof still traverses the WHOLE retained seed once per run;
upload-count and upload-byte limits do not bound that verification work.

`signal` and `timeoutMs` apply to the full awaited run. Remaining monotonic
budgets are forwarded rather than reset per request. Cancellation never races
away from started SQL, transport or confirmation. A callback that ignores its
signal can delay settlement. Errors preserve the failing phase and root ID,
with the local cause available for reconciliation; causes may contain sensitive
application information and must not be logged indiscriminately.

## Verification boundary

The transfer-contract suite executes the new production orchestration module
with explicit SQLite-backed source/storage/receiver boundary fixtures. Those
fixtures are substituted only by `bootstrap-transfer-loader.mjs`; they are NOT
the production bootstrap, outbox-store, fanout, order or apply implementations.
Node's actual SQLite Session extension supplies the bytes and applies receiver
rows; real SQLite transactions and separate files exercise the boundary cuts.

76 tests pass on Node 22.16.0 / SQLite 3.49.1 with no failures/skips. Coverage
includes bounded resume, no staging ACK reclamation, lost responses, source and
receiver confirmation failure, corrupted receipts/progress, ordered binding,
fanout delegation, payload preflight, cancellation drain and real SQL rollback.
Three child processes are SIGKILLed after staging, receiver commit and source
ACK, then fresh owners resume the retained state. These are fixture-backed
orchestration results, not execution of the current production receiver/store.

```sh
node --experimental-loader=./packages/sdk/tests/helpers/bootstrap-transfer-loader.mjs \
  --test packages/sdk/tests/changeset-bootstrap-transfer-contract.test.mjs
```

TypeScript 5.8.3 checks cover the new module against declaration fixtures of the
used APIs read at 95b42608ba465f7af0fa7006bba3993ba35299eb, with strict mode, exact
optional properties, unchecked-index and unused checks. A full SDK typecheck,
production-module integration run, HTTP bootstrap transport, Rust/WASM/MVCC,
browser storage, TLS deployment and physical power-loss qualification remain
unperformed. The patch changes no dependency, native storage or writer defaults.
