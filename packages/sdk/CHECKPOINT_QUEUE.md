# Checkpoint-before-acknowledgement transaction queues

Opt in when a successful queued transaction must also publish its committed
SQLite image to the existing IndexedDB snapshot store:

```ts
import { FrankenDBQueue, FrankenCheckpointCommitError } from "@frankensqlite/sdk";

const queue = await FrankenDBQueue.open(
  { dbName: "offline-notes", persistence: "indexeddb-snapshot" },
  { checkpointOnCommit: true, maxPendingJobs: 64 },
);

await queue.transaction(async tx => {
  await tx.execute("CREATE TABLE IF NOT EXISTS notes(id INTEGER PRIMARY KEY, body TEXT)");
  await tx.execute("INSERT INTO notes(body) VALUES (?)", ["A saved note"]);
});
// Both SQL COMMIT and snapshot publication have now been acknowledged.
console.log(queue.snapshotRevision);
await queue.close();
```

`checkpointOnCommit` is immutable for a queue and defaults to `false`. It
requires the resolved `indexeddb-snapshot` mode, not `memory`, `opfs`, or a
promise of page-level persistence. Invalid policies reject; a mode mismatch
closes the newly opened database before rejection and retains cleanup errors.
Ordinary `FrankenDB` calls and queues without this option remain unchanged.

## Ordering and acknowledgement

Every successful `transaction()` and `transactionWithRetry()` job checkpoints
its final committed image before settling and releasing its FIFO slot. All
SQL retries and rollbacks happen first; checkpointing is OUTSIDE the replay
loop. Later transactions, exports, checkpoints and close cannot overtake that
publication. Read-only transaction callbacks checkpoint too: this is an
explicit per-transaction policy, not a SQL write detector. Batch related work
inside one transaction to avoid repeatedly exporting the whole database.

The underlying snapshot store resolves only after IndexedDB transaction
completion, not after an individual put succeeds. It uses its existing
strict-durability policy, validated image envelope and revision compare-and-swap.
An older worker cannot silently replace a newer worker's checkpoint. This is
whole-image persistence, NOT cross-tab native MVCC, row-level conflict merging,
an OPFS/page VFS, or an unconditional guarantee against browser eviction and
platform failure. Images still have the snapshot store's 64 MiB bound, and
export, copying and hashing take work and memory proportional to database size.

Cancellation or a SQL timeout before COMMIT follows ordinary rollback rules
and publishes nothing. Once SQL COMMIT succeeds, cancellation cannot undo it
or abandon saving its image. The checkpoint's real outcome is awaited even if
a signal or deadline expires. SQL callback timeouts do not impose an interrupt
or wall-clock bound on subsequent snapshot publication.

## SQL committed, checkpoint unacknowledged

SQL COMMIT and snapshot publication are separate operations. If publication
fails, the job rejects with `FrankenCheckpointCommitError<T>`:

- `code`: `ERR_FSQLITE_COMMITTED_CHECKPOINT_FAILED`.
- `sqlCommitted: true`: do NOT repeat the transaction callback.
- `checkpointConfirmed: false`: publication was not acknowledged, NOT proof
  that nothing was saved. A response may have been lost after publication.
- `value`: the successful callback's return value, retained for recovery.
- `previousRevision`: the last acknowledged revision before this checkpoint.
- `cause`: the original storage/export/transport failure, unchanged.

Even a BUSY-shaped cause cannot trigger SQL replay after this boundary. The
committed rows and trigger effects remain in the live connection if it is
usable; they are not rolled back or represented as a failed SQL transaction.

The queue sets `stats.checkpointRecoveryRequired`. Transaction jobs and new
subscriptions are fenced before SQL, including jobs accepted before the
failure. They reject with `ERR_FSQLITE_CHECKPOINT_RECOVERY_REQUIRED`; its cause
retains the original committed-checkpoint error. Export, explicit checkpoint
and close remain available. Unsubscribe still stops delivery immediately.

Await `queue.checkpoint()` to publish that SAME committed image and clear the
fence. Only acknowledged publication clears it; a rejected, cancelled waiting,
failed-CAS or otherwise failed checkpoint does not. There are no hidden retries
or unbounded recovery jobs. Wait for recovery before submitting new transaction
jobs. A recovery checkpoint already queued before failure executes in FIFO
order and may unblock jobs queued behind it.

```ts
try {
  await queue.transaction(tx => tx.execute("INSERT INTO notes(body) VALUES (?)", ["Once"]));
} catch (error) {
  if (!(error instanceof FrankenCheckpointCommitError)) throw error;
  // SQL committed. Do not repeat INSERT or the callback's external effects.
  // Resolve recoverable quota/storage problems, then:
  await queue.checkpoint();
}
```

A competing-writer CAS conflict needs explicit reconciliation, not blind retry:
export the losing connection's image, reopen the authoritative checkpoint and
merge application changes. A fatal transport failure may make export/recovery
unavailable, and a lost acknowledgement may leave publication unknown.

### Validated receipts and uncertain revision lineage

The public `FrankenDB` boundary validates saved metadata during initialization
and checkpoint acknowledgement. Revision tokens must be UUID v4, hashes must
be 64 lowercase hexadecimal characters, and image lengths must be whole
512-byte units between 512 bytes and 64 MiB. Own scalar fields are captured
into an immutable receipt; accessor fields are rejected without calling them.
Each checkpoint's parent must equal the last acknowledged revision at response
acceptance. Normally queued checkpoints extend that chain in FIFO order.

Malformed or out-of-order receipts reject with `ERR_FSQLITE_SNAPSHOT_RECEIPT`
and preserve the last acknowledged revision. A lost earlier acknowledgement
can also cause a later receipt to skip a parent. Once lineage is unknown, this
database refuses further checkpoint requests until reopened; even an older
in-flight response cannot clear that uncertainty. Export the live image, reopen
the authoritative stored snapshot and reconcile. An automatic queue checkpoint
wraps this error in `FrankenCheckpointCommitError` and retains its recovery fence.
Ordinary quota failures and known failed CAS publications keep their existing
recovery behavior; a valid receipt is still required before clearing the fence.

These checks validate the acknowledgement contract, not its authenticity or
the exported bytes. The worker/store still owns image validation and hashing.
A syntactically valid hash is not independent proof of storage publication.

`close()` drains accepted work and releases resources. It does not silently
retry publication. With unresolved checkpoint recovery, close REJECTS with the
committed-checkpoint error after cleanup; if cleanup also fails, an aggregate
retains both causes. Export BEFORE closing when recovery is impossible. Close
remains idempotent and returns one shared promise.

## Change notifications and accounting

Existing change subscriptions describe LOCAL SQL commits, not durable storage
receipts. A SQL commit still emits its one local invalidation if its checkpoint
later fails. Recovering that checkpoint emits no duplicate invalidation, and
listener failures cannot undo the write or replay it. Await the transaction
job for checkpoint acknowledgement rather than using a notification as proof.

An unacknowledged checkpoint counts as one failed job even though its SQL
committed; inspect the typed error rather than treating every failed job as
rolled back. Active queue capacity is held through publication/recovery work.
Fenced waiting jobs fail without executing their callbacks or journal SQL.

## Verification

```sh
node --test packages/sdk/tests/checkpoint-queue.test.mjs
node --test packages/sdk/tests/transaction-retry.test.mjs
```

Requires Node 22.16+ with `node:sqlite` and TypeScript, optionally selected by
`FSQLITE_TYPESCRIPT_MODULE`. The new suite executes production SDK, queue,
journal and snapshot-store code. SQL and exported/reopened database images use
Node's SQLite reference; IndexedDB uses the repository's explicit transaction
MODEL. Cases include stored-image integrity, CAS contention, completion versus
put-success, post-put abort, lost acknowledgement, retries, cancellation,
recovery fences, queue capacity, notifications and cleanup.

This does not certify browser IndexedDB, worker IPC, WASM, native FrankenSQLite
MVCC, Rust builds, or full-workspace TypeScript checking. The browser/WASM
acceptance gates remain separate. No GitHub Actions settings are changed.
