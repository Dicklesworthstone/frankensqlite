# Durable job transactions and callback ownership

`DurableJobQueue.enqueueWith()` couples application SQL to job publication.
`completeWith()` couples application SQL to a live, fenced job completion.
Their callback executors admit SQL only while the callback is active. Every
admitted `execute()` or `query()` settles before publication/commit, the final
completion lease check, or propagation of a callback failure for rollback.

A failed admitted statement aborts the operation even when application code
catches its rejection. A callback failure takes precedence over child failures,
but still waits for children to finish. Handles retained past callback completion
reject with `ERR_FSQLITE_JOB_SCOPE_ENDED` before reaching the SQL adapter; they
cannot be reused in the queue's postlude or another transaction. The first SQL
failure is retained, including falsy rejection values, not an unbounded error list.

Always await statements in application code. This drain is a correctness boundary,
not permission to create unlimited outstanding work: transaction ownership,
statement admission limits and execution ordering remain the database adapter's
responsibility. No additional SQL queue, writer lock, retry, timer or background
worker is introduced. A hung statement still requires the owner's cancellation
policy; the helper cannot abandon it safely.

Completion expiry is checked after draining SQL. If the lease expires during that
work, its application writes and completion both roll back. Duplicate enqueue
still skips application work. Completed or replaced leases cannot rerun completion
callbacks. A lost post-COMMIT response is uncertain, not proof of rollback; recover
from the retained job state, never rerun business work blindly.

Callbacks are trusted SQL composition, not a sandbox. They must not commit or roll
back the owner, change queue metadata, execute via another handle on the same
connection, reenter the queue, or perform external effects that SQL cannot undo.
The host must provide a transaction that settles after commit/rollback, including
any required same-database storage confirmation. Independent browser snapshots
are not a shared live queue. No native SQL registration or new schema is implied.

## Verification

From the repository root, using Node 22.16.0 or a compatible Node release:

```sh
node --experimental-transform-types --test \
  packages/sdk/tests/durable-job-callback-lifetime.test.mjs
```

The 30 tests execute the production queue with real reference SQLite statements,
transactions, constraints and savepoints. Explicit barriers delay actual SQL;
no SQL interpreter or queue implementation is substituted. The unchanged queue
fails 24 cases. Coverage includes execute/query RETURNING, callback and statement
failures, delayed lease expiry, late handles, duplicate and lost-commit responses,
UTF-8/UTF-16LE/UTF-16BE, and deferred foreign-key failures at COMMIT. Strict
TypeScript 5.8.3 checking covers the actual queue module, which has no imports.
This is SDK-component/reference-SQL evidence, not execution of FrankenSQLite
Rust/WASM/MVCC, the full worker package, browser durability or physical power loss.

## Atomic follow-up jobs

`queue.completeAndEnqueue(lease, next, work, result?)` completes the parent,
runs scoped application SQL and retains all follow-up jobs in one transaction.
This closes the failure window between acknowledging a job and separately
scheduling its successors. It supports multiple queues on the SAME database;
there is no distributed transaction across databases or external services.

```ts
await queue.completeAndEnqueue(lease, [
  { queue: 'notifications', id: `notify:${lease.id}`, payload: notificationJson },
  { queue: 'indexing', id: `index:${lease.id}`, payload: indexJson },
], async tx => {
  await tx.execute('UPDATE documents SET processed=1 WHERE id=?', [documentId]);
}, 'processed');
```

The result contains the callback `value` and frozen `jobs`, in input order.
Every child result has the ordinary `inserted` flag and retained job snapshot.
Identical existing child requests deduplicate using the existing enqueue rules;
completed/dead/cancelled children are not revived. Conflicting payload, priority,
maximum attempts, or an explicitly different schedule rolls back ALL newly
inserted children, business effects and parent completion. Choose child ids that
permanently identify the same work. Deduplication is not an all-parent join barrier.

The parent lease is fenced before work and after all child publications. If it
expires while business SQL or child publication runs, the group rolls back.
A stale parent never invokes the callback. A later child conflict or COMMIT
failure cannot leave a committed prefix. Nested results remain provisional until
the outer transaction commits. A lost completion acknowledgement requires
inspection of retained parent/child state: retry with the old lease rejects,
rather than repeating application work or pretending to prove why it completed.

Input is captured before admission: 1..128 children, at most 1 MiB per payload,
and at most 4 MiB total payload UTF-8 bytes. Queue and job identifiers retain
existing limits. Duplicate `(queue,id)` pairs in a single batch and a direct
self-continuation reject before SQL. This does not detect arbitrary cycles across
separate workflows. Limits describe logical payloads, not total SQLite memory,
file size or RSS. No new schema, dependency, writer mutex or retry queue is added.

## Existing worker integration

The already exported `DurableJobCompletion` now accepts optional `next` jobs:

```ts
const worker = DurableJobWorker.start(queue, async (lease, context) => {
  const output = await computeOutput(lease.payload, context.signal);
  return {
    result: 'processed',
    next: [{ queue: 'notifications', id: `notify:${lease.id}`, payload: output }],
    apply: async (tx, scope) => {
      scope.checkpoint();
      await tx.execute('INSERT INTO processed_jobs(id) VALUES(?)', [lease.id]);
    },
  };
}, { owner: 'worker-1', stopWhenIdle: true });
await worker.done;
```

The worker captures and validates `next` before joining an in-flight heartbeat;
subsequent mutation of the handler's objects cannot change the chosen successors.
Legacy queue adapters continue supporting ordinary results. A requested
continuation on an adapter without `completeAndEnqueue` fails the job before
application SQL; it is never silently downgraded to ordinary completion.

The worker also joins admitted apply SQL before its final cancellation/monotonic
lease check, for both ordinary and continuation completion. This keeps a dropped
statement from escaping a cancellation that arrives while that statement runs.
Graceful stop drains an admitted group. Once callback work has finished and final
queue publication is underway, stop (including abort) still joins the transaction:
the entire group may commit. Cancellation is not proof of rollback. A failed or
uncertain completion stops the worker under its existing reconciliation policy;
it does not automatically reschedule the parent. No additional daemon is started.

### Combined verification

```sh
node --experimental-transform-types \
  --experimental-loader=./packages/sdk/tests/helpers/production-source-loader.mjs \
  --test packages/sdk/tests/durable-job-callback-lifetime.test.mjs \
  packages/sdk/tests/durable-job-continuations.test.mjs \
  packages/sdk/tests/durable-job-worker-continuations.test.mjs
```

All 73 tests pass: 30 callback-lifetime, 30 queue-continuation and 13 actual-worker
integration cases. The source loader resolves extensionless TypeScript imports
only. Neither production queue nor worker is replaced. SQLite supplies real SQL
and transaction ownership, including constraints and independently opened WAL
readers/writers. Tests cover a three-stage consumed workflow, cross-queue fanout,
exact deduplication, delayed heartbeat receipt/input ownership, cancellation,
graceful stop, uncertain completion, 4 MiB admission, schema constraints,
concurrent parent completion and all three database encodings.

Eight child processes report their requested IPC boundary before being SIGKILLed:
first/last child insertion, parent completion before COMMIT, and committed before
response, under WAL and DELETE journals. Fresh owners recover all-or-none
publication. A watchdog kill fails the test rather than becoming a passing crash
receipt. This establishes process-death recovery, not loss of unsynced storage.
Strict TypeScript checking includes the actual queue and worker modules. These
are targeted component tests, not a rerun of the entire SDK, FrankenDB/queue owner,
FrankenSQLite native/Rust/WASM/MVCC, browser checkpoints, or power-loss tests.

## Main-database storage and lease authority

All queue table reads and writes explicitly address
`main.__fsqlite_durable_jobs_v1`. The two queue indexes are also explicitly
created in `main`. The exported `DURABLE_JOBS_TABLE` remains the unqualified
table-name constant for compatibility; it is not an SQL fragment promising
automatic schema resolution for caller-written SQL.

This fixes a reproduced silent durability failure: with a structurally compatible
TEMP table of the same name, `enqueue()` returned `inserted: true` while the real
main table remained empty. That job vanished when the connection closed. A TEMP
copy of a leased row could also authorize completion after a different owner
cancelled the main job. The repair binds fencing, reads, statistics, expiry
recovery and cross-queue continuation publication to the same persistent state.

Existing same-named TEMP tables/views and attached tables are left untouched.
Opening a queue creates/checks its indexes in main even when TEMP has identically
named indexes. A live queue whose main table is renamed or lost rejects rather
than borrowing an attached replacement. No shadow table is dropped or migrated;
already-lost jobs cannot be reconstructed by this change.

This is namespace isolation, not a sandbox or protection against trusted code
rewriting main queue metadata. Application callback SQL still chooses its own
schemas. The adapter must still own transactions and provide genuine storage
confirmation; a main database opened in memory does not become file-durable.

### Storage-isolation verification

Run the preceding combined command with
`packages/sdk/tests/durable-job-storage-isolation.test.mjs` as an additional test
file. All 96 targeted cases pass: the original 73 plus 23 new isolation cases.
The unchanged pre-fix queue fails 22 of the 23 isolation cases. The new cases
exercise file reopen under WAL/DELETE in all three SQLite encodings, incompatible
TEMP tables/views/indexes, the entire claim/renew/fail/complete/cancel/reap
lifecycle, cancellation by an independent file-backed owner, callback-created
shadows, missing-main/attached fallback, lost commit responses, and actual worker
continuations. They use actual queue/worker modules over reference SQLite.

Existing SQL-interruption selectors now match the explicitly qualified table;
their assertions, requested cuts and watchdog failures are unchanged. All eight
original continuation SIGKILL/reopen cases rerun successfully; the 23 isolation
cases do not add new process-kill scenarios. Strict TypeScript checks cover the
actual queue/worker modules. These are targeted SDK-component tests, not a full
legacy SDK-suite rerun, native FrankenSQLite execution or power-loss evidence.
