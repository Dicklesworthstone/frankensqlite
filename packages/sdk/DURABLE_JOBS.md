# Durable SQL jobs

`DurableJobQueue` persists work and lease ownership in ordinary SQL. It is not
`FrankenDBQueue`: that class owns a connection and schedules JavaScript callbacks
in memory. The two compose: submit durable job operations through a
`FrankenDBQueue` to retain its bounded admission and transaction ownership.

```ts
import { FrankenDBQueue, DurableJobQueue } from '@frankensqlite/sdk';

const database = await FrankenDBQueue.open(
  { dbName: 'work', persistence: 'indexeddb-snapshot' },
  { checkpointOnCommit: true, maxPendingJobs: 64 },
);
const jobs = await DurableJobQueue.open(database, 'index-documents');
await jobs.enqueue({ id: 'document:123:v4', payload: JSON.stringify({ documentId: 123 }) });
const lease = await jobs.claim('worker-7', 30_000);
if (lease !== null) {
  // Perform application work outside the claim transaction.
  // Renew before expiry while long-running work continues.
  await jobs.complete(lease, 'indexed');
}
await database.close();
```

## Guarantees and boundaries

The host must execute every supplied callback in one atomic transaction and
settle only after commit or rollback. `FrankenDB` and `FrankenDBQueue` implement
the narrow `DurableJobDatabase` interface. A native adapter can implement the
same interface against a shared file. Do not provide a callback-retrying
adapter. This API never catches and blindly retries transaction or commit
errors.

Storage durability is the host's policy. An in-memory connection remains
volatile. `indexeddb-snapshot` needs acknowledged checkpoints to survive worker
termination; `checkpointOnCommit: true` supplies that policy through
`FrankenDBQueue`. A committed-but-unacknowledged checkpoint error propagates
unchanged. Recover or reconcile it; do not replay SQL or external work merely
because the acknowledgment was lost.

Independent browser snapshot connections are separate live databases, NOT a
shared multi-writer queue. Their publication compare-and-swap rejects stale
images; it does not coordinate live job execution across tabs. Keep one owner
of a browser queue, or supply an actual shared transactional SQL backend.

Delivery is **at least once**. Expiry cannot stop an old process from performing
external side effects. Use the stable job id as an external idempotency key.
A random token, owner, attempt, queue, job id and the persisted deadline fence
all completion, failure and renewal mutations. A stale receipt cannot finish a
reclaimed job, even when a worker name or deleted job id is reused. Tokens are
ownership receipts, not protection against a caller with direct SQL access.

Leases use wall-clock milliseconds sampled inside the transaction, after any
scheduler wait. Workers sharing storage must agree on time. Clock skew and
clock jumps affect lease eligibility; there is no distributed-clock guarantee.
A heartbeat extends but never shortens the persisted deadline. The receipt's
old `expiresAt` value is informational after renewal.

## Operations

`enqueue({ id, payload, priority?, availableAt?, maxAttempts? })` requires a
stable caller-provided id. The id is scoped to the queue. Repeating identical
input returns the existing job and `inserted: false`, without resetting state,
attempts, results or retry scheduling. Different payload, priority, attempt
limit or explicitly supplied initial schedule yields
`ERR_FSQLITE_JOB_ID_CONFLICT`. Payload, result and failure text each have a
1 MiB UTF-8 limit; JSON should be encoded by the application.

`claim(owner, leaseMs = 30000)` chooses eligible work by descending priority,
then availability, creation time and id. It increments attempts and creates a
fresh token atomically. It may directly reclaim an expired non-final attempt.
A null result means no currently eligible non-exhausted job, not an empty table.

`claimBatch(owner, { limit?, leaseMs?, maxPayloadBytes? })` claims an ordered
prefix in **one transaction** and, with checkpoint-on-commit, one checkpoint.
It defaults to 16 jobs, 30-second leases and 4 MiB of returned UTF-8 payload.
The hard limits are 128 receipts and 64 MiB; the payload budget must allow at
least 1 MiB so that a single valid job can always fit. Only candidate ids are
selected up front; payloads are loaded individually, with at most one lookahead
job when the byte limit is reached. This bounds returned payload, not total
database memory, UTF-16 string storage, or process RSS.

A byte-limited batch stops at the first job that does not fit rather than
skipping higher-priority work. Unclaimed candidates do not consume attempts.
Every receipt has a distinct token. A failure midway through claiming rolls
back the entire SQL transaction; a lost commit/checkpoint acknowledgement still
propagates as an unknown outcome, never as partial success. Do not prefetch more
jobs than the worker can process or renew before their shared deadline.

`renew(lease, leaseMs = 30000)`, `complete(lease, result = null)` and
`fail(lease, error, retryDelayMs = 0)` require an unexpired current lease. At the
exact deadline, the lease is expired. Failure releases work to a delayed retry
or marks the final attempt `dead`. Lease durations are bounded to one day;
timestamps and arithmetic must fit JavaScript's safe-integer range.

`reapExpired(limit = 100)` recovers at most 1,000 expired leases per call. It
requeues remaining attempts and dead-letters workers that crashed on their
final attempt. Call it periodically; `claim` intentionally does not perform an
unbounded recovery sweep. `cancel(id)` cancels ready or leased work and fences
its handler; terminal states are not overwritten. `get(id)` reads current
state without exposing the token.

`stats()` reads actual SQL state counts in a single transaction. It reports
ready, leased, completed, dead and cancelled totals, plus currently available
ready jobs and expired leases. An expired lease remains `leased` until reclaimed
or reaped; these counts do not fabricate a state transition. This is an explicit
queue scan, not an inexpensive scheduler counter or an engine-health check.

## Atomic application handoffs

Do not commit an application change and only then enqueue the corresponding
work: a crash between those operations can lose the job. `enqueueWith` writes
both in one transaction. A duplicate stable id skips the callback and returns
`inserted: false, value: undefined`; a conflicting job definition rejects before
the callback runs.

```ts
const submitted = await jobs.enqueueWith(
  { id: 'document:123:v4', payload: JSON.stringify({ documentId: 123 }) },
  async tx => {
    await tx.execute('UPDATE documents SET body = ? WHERE id = ?', ['new text', 123]);
    return 123;
  },
);
// submitted.value is 123 for a new job, undefined for a duplicate.
```

Likewise, `completeWith` couples a current lease's completion with its database
effects. It conditionally writes the ownership row before calling application
code and checks the persisted lease again after the callback. A stale lease
never runs the callback. Callback failure, SQL error or expiry detected by the
second check rolls back **all** SQL in that transaction, including application
effects. The returned promise resolves to the callback value only after the
host commits.

```ts
const lease = await jobs.claim('worker-7');
if (lease !== null) {
  // Parse, compute and contact other systems OUTSIDE the transaction.
  await jobs.completeWith(lease, async tx => {
    await tx.execute('INSERT INTO indexed_documents(job_id) VALUES (?)', [lease.id]);
    return lease.id;
  }, 'indexed');
}
```

Both callbacks must use only the supplied transaction, await all their SQL and
not retain the transaction after return. Never call `jobs.*` or the owning
`FrankenDBQueue` from inside a callback; that would queue a sibling behind the
transaction waiting for it. Do not perform external side effects in these
callbacks: transaction rollback cannot undo them. Normal commit contention and
committed-but-unacknowledged publication errors propagate unchanged, without
automatic callback replay. Reconcile an unknown outcome before retrying.

The reserved SQL table is `__fsqlite_durable_jobs_v1`, exported as
`DURABLE_JOBS_TABLE`; its two indexes support eligibility and expiry scans.
The schema is installed transactionally. Application SQL must not change its
schema or bypass state transitions.

## Validation

The Node suite executes this production module against actual SQLite, including
on-disk reopen, expiry boundaries, ABA fencing, retry exhaustion, bounded
reaping, cancellation, input capture, transactional outbox/completion rollback,
live SQL diagnostics and committed-but-unacknowledged errors:

```sh
node --experimental-loader=./packages/sdk/tests/helpers/source-loader.mjs \
  --test packages/sdk/tests/durable-jobs.test.mjs packages/sdk/tests/durable-jobs-thread.test.mjs
```

The thread suite uses four independent SQLite connections to the same file,
checks disjoint batched claims and unique application effects, and forcibly
terminates workers after a committed claim and during uncommitted completion.
The latter verifies that an abandoned application transaction leaves no partial
effects and the persisted lease can subsequently be reclaimed. These are
SQLite-reference worker-termination tests, not machine power-loss tests.

It requires a Node release with `node:sqlite` and the workspace TypeScript
module. This reference suite is not FrankenSQLite engine conformance,
cross-process SSI certification, browser persistence certification, or a
power-loss test.
