# Confirming an uncertain checkpoint without replaying SQL

`db.recoverCheckpoint()` and `queue.recoverCheckpoint()` can reconstruct the
receipt for a failed checkpoint acknowledgement when the worker is still usable
and the exact publication remains in storage. They read and hash the stored
image. They do not execute SQL, export the current database, write another
snapshot, or rerun a transaction callback.

This closes the recoverable case where SQL COMMIT and snapshot publication
succeeded, but a storage acknowledgement failed or the worker's correlated
response was malformed. It does not turn every persistence error into success.

```ts
import { FrankenDBQueue, FrankenCheckpointCommitError } from "@frankensqlite/sdk";

const queue = await FrankenDBQueue.open(
  { dbName: "notes", persistence: "indexeddb-snapshot" },
  { checkpointOnCommit: true },
);

try {
  await queue.transaction(async tx => {
    await tx.execute("INSERT INTO notes(body) VALUES (?)", ["Remember this"]);
  });
} catch (error) {
  if (!(error instanceof FrankenCheckpointCommitError)) throw error;
  // The SQL already committed. NEVER rerun its callback to repair this error.
  // This rejects if the stored image cannot confirm the exact publication.
  const receipt = await queue.recoverCheckpoint();
  console.log(receipt.revision, receipt.sha256);
}
```

After successful recovery the queue's `checkpointRecoveryRequired` fence is
cleared, and later transaction jobs may run. The original failed job remains
failed; its `FrankenCheckpointCommitError.value` still holds the callback value.
Confirmation neither increments the change sequence nor delivers duplicate
notifications. It skips subscription-maintenance SQL even when abandoned
subscriptions need cleanup. That maintenance waits for an ordinary job.

## Evidence and failure rules

A supporting worker advertises `checkpointRecovery: 1` when opening a snapshot
session. The SDK then chooses a UUID-v4 publication identity before posting each
checkpoint. The store uses that identity as the new revision, preserving its
existing compare-and-swap parent check and transaction-completion barrier.
Successful receipts must echo the requested identity and extend the last
acknowledged parent. Older clients still receive generated revision tokens.

For a failed acknowledgement the SDK retains one bounded candidate: the
publication identity and last acknowledged parent. `recoverCheckpoint()` sends
those values through the ordinary bounded worker FIFO. The worker loads a
consistent stored record, waits for the IndexedDB read to complete, validates
the envelope/database header and size, and verifies SHA-256 over its bytes.
Only an exact identity and parent match returns a receipt. The worker also
refuses to regress its own revision when a later local checkpoint advanced it.
The SDK validates the returned receipt again before changing its revision or
clearing uncertainty.

A later writer can publish immediately after the read. Confirmation establishes
what was observed, not a lock on the storage head; the next save still uses CAS
and rejects if storage has advanced. SHA-256 detects corruption, not malicious
same-origin code. Recovery reads the entire bounded image (up to 64 MiB), and
its hashing/allocation cost is proportional to image size.

`ERR_FSQLITE_SNAPSHOT_NOT_CONFIRMED` means the stored head cannot confirm this
publication. It does NOT mean the SQL rolled back or the image never published:
a later writer, origin eviction, or another outstanding checkpoint may have
replaced it. Corrupt bytes/envelopes reject with the store's corruption error.
Neither failure replaces stored data or clears the queue's fence. Export the
live image and reopen/reconcile authoritative storage when confirmation cannot
succeed. Recovery does not merge competing browser snapshots.

For a known pre-publication failure such as quota exhaustion, confirmation will
not find the new identity. The existing explicit `checkpoint()` retry remains
available after fixing the failure. Never replace this with transaction replay.
Malformed receipt or lineage failures still block new publication until exact
readback succeeds or the session is reopened.

## Lifetimes and compatibility

`checkpointRecoverySupported` reports the negotiated capability on both database
and queue. With an older worker, ordinary checkpoints retain their previous
behavior, but recovery rejects with `ERR_FSQLITE_SNAPSHOT_RECOVERY_UNAVAILABLE`.
Unsupported/malformed capability versions fail opening rather than inventing
support. Memory-only sessions do not advertise snapshot recovery.

An absent candidate rejects with `ERR_FSQLITE_SNAPSHOT_RECOVERY_EMPTY`. Recovery
refuses while checkpoint responses remain outstanding, and new checkpoints
cannot enter while recovery is active (`ERR_FSQLITE_SNAPSHOT_RECOVERY_PENDING`,
or the already-retained receipt error). Concurrent database recovery callers
share one promise/read. A failed confirmation retains the candidate for another
readback; late replies cannot erase the earlier uncertainty. Multiple concurrent
checkpoints with missing intermediate receipts may require reopening rather
than skipping a revision.

Queue recovery is one FIFO job and accepts the usual `signal` and
`waitTimeoutMs` waiting controls. A queued abort/timeout starts no recovery.
Once readback starts, the real result remains authoritative: it is not abandoned
on a late abort, capacity stays reserved, and close waits for it. Failed recovery
leaves the original committed-state error available; unresolved queue close
retains its existing rejection behavior. Managed transaction ownership also
applies to recovery, so it cannot run through a foreign or callback-owned
connection handle.

A crashed/disposed worker still requires reopening. A silently missing response
must first be reported/settled by the transport; this API does not add a timeout
that abandons an in-flight request or revive a dead worker.

The returned receipt describes the ORIGINAL saved image. Direct `FrankenDB`
callers may have made later in-memory writes after a checkpoint failed. Readback
does not save those writes or assert that all current memory is persisted. Queue
checkpoint-on-commit fencing prevents later transaction jobs from making such
writes until recovery, but direct database callers must still await a new
checkpoint for any later changes.

## Verification

```sh
node --experimental-loader=./packages/sdk/tests/helpers/source-loader.mjs \
  --test packages/worker/tests/snapshot-publication.test.mjs \
  packages/worker/tests/checkpoint-recovery.test.mjs \
  packages/sdk/tests/checkpoint-recovery.test.mjs
```

The suites execute production storage, host, admission, SDK, queue and client
code. SDK integration uses actual Node worker-thread messages, with the existing
Node SQLite reference adapter and the repository's explicit IndexedDB transaction
MODEL. Stored real SQLite images are reopened and checked for integrity and row
contents. This is not a browser storage, browser worker entrypoint, native Rust,
or WASM certificate. Strict TypeScript checking of the affected dependency graph
uses the repository configuration and actual core declarations; it is not a
full-workspace build.

## Reopening after worker or transport failure

Both `opfs-snapshot` and `indexeddb-snapshot` support exact-image reopening.
After a failed checkpoint settles, `db.pendingCheckpointRecovery` (also exposed
by `FrankenDBQueue`) returns an immutable `CheckpointRecoveryIdentity`, or null
when no recoverable candidate is known. It includes the storage backend, database
name, publication UUID and last acknowledged parent. It remains readable after
worker disposal and failed queue close. It is an identity, **not a success
receipt**; retain it before retrying any operation that can replace the candidate.

Pass this identity as `requireCheckpoint` when opening a new database or queue:

```ts
// Run after a failed checkpoint/queue job has settled, not while it is pending.
const required = queue.pendingCheckpointRecovery;
if (required === null) throw new Error("No checkpoint identity; reconcile manually");

// Release the old owner. An unresolved checkpoint keeps close() rejecting.
try { await queue.close(); } catch { /* The original failed outcome is retained. */ }
const reopened = await FrankenDBQueue.open(
  { dbName: required.path, persistence: required.persistence, requireCheckpoint: required },
  { checkpointOnCommit: true },
);
// Reaching here means the exact checkpoint was restored. Do NOT rerun the
// original committed callback. Continue with new work on reopened instead.
```

The precondition is captured and validated before allocating a worker or
transferring import bytes. It must identify the requested snapshot backend and
name and cannot be combined with an initialization image. The new worker loads,
validates, hashes and imports authoritative bytes normally. The SDK exposes the
handle only when the restored revision AND parent match the required identity.
It never upgrades a stale in-memory image's revision to a newer stored token.
The next checkpoint therefore extends the actual imported image, with the
ordinary compare-and-swap protection against a later writer.

Absent storage, an older checkpoint, incorrect parent lineage, or a superseding
checkpoint rejects opening with `ERR_FSQLITE_SNAPSHOT_NOT_CONFIRMED`. A corrupt
envelope or checksum rejects with the storage corruption error. In particular,
an unsuccessful reopen does not prove the old publication never happened.
Reconcile competing data; there is no automatic merge, historical revision
lookup, or permission to repeat application effects.

The recovery identity is JSON-serializable but is not persisted by the SDK.
Whole-page/application death also loses it unless the application retained it
elsewhere. No candidate is exposed while checkpoints remain outstanding, and
older workers without publication identities still cannot supply one. This
feature does not recover in-memory writes made after the saved checkpoint.

The `packages/sdk/tests/opfs-persistence.test.mjs` suite covers both backends
through the production SDK and host, real Node SQLite images, and deterministic
storage models. Its fatal-delivery tests retire the original transport and
reopen through a new host without replaying SQL or republishing a snapshot.
These are not actual browser, WASM, machine-crash or power-loss tests.
