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

The reserved SQL table is `__fsqlite_durable_jobs_v1`, exported as
`DURABLE_JOBS_TABLE`; its two indexes support eligibility and expiry scans.
The schema is installed transactionally. Application SQL must not change its
schema or bypass state transitions.

## Validation

The Node suite executes this production module against actual SQLite, including
on-disk reopen, expiry boundaries, ABA fencing, retry exhaustion, bounded
reaping, cancellation, input capture and committed-but-unacknowledged errors:

```sh
node --experimental-loader=./packages/sdk/tests/helpers/source-loader.mjs \
  --test packages/sdk/tests/durable-jobs.test.mjs
```

It requires a Node release with `node:sqlite` and the workspace TypeScript
module. This reference suite is not FrankenSQLite engine conformance,
cross-process SSI certification, browser persistence certification, or a
power-loss test.
