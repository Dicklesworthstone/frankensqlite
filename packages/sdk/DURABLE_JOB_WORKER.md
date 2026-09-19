# Managed durable job workers

`DurableJobWorker` consumes the SQL jobs described in `DURABLE_JOBS.md`. It owns
handler lifetimes, polling, automatic lease renewal and periodic bounded expiry
recovery. It does not own or close the database, create operating-system threads,
or make independently imported browser snapshots a shared live queue.

```ts
import { FrankenDBQueue, DurableJobQueue, DurableJobWorker } from '@frankensqlite/sdk';

const database = await FrankenDBQueue.open(
  { dbName: 'jobs', persistence: 'indexeddb-snapshot' },
  { checkpointOnCommit: true },
);
const jobs = await DurableJobQueue.open(database, 'documents');
await jobs.enqueue({ id: 'document:123:v4', payload: JSON.stringify({ id: 123 }) });
const worker = DurableJobWorker.start(jobs, async (lease, context) => {
  context.checkpoint();
  const input = JSON.parse(lease.payload);
  // Await application work here. Pass context.signal to cooperative operations
  // and use lease.id as their idempotency key; external effects remain at least once.
  return JSON.stringify({ processed: input.id });
}, { owner: 'indexer-1', concurrency: 4, stopWhenIdle: true });

await worker.done;
await database.close();
```

## Execution and bounds

Each slot claims at most one job and processes it before claiming again. There
is no queue of prefetched leases waiting for a handler. `concurrency` defaults
to 1 and is bounded to 64. These are overlapping asynchronous handlers, not
parallel CPU execution. Each job payload is already bounded by DurableJobQueue;
this is not a bound on application allocations or database memory.

The host must accept concurrent transaction requests when multiple slots or a
heartbeat are active. Use `FrankenDBQueue` over the SDK database, or an adapter
with equivalent transaction ownership. Do not share a bare connection with
uncoordinated application transactions. Do not put a callback-retrying adapter
under the job queue.

A handler returns a string (up to 1 MiB UTF-8), null, undefined, or the atomic
completion object described below. Successful work is completed using its
persisted ownership fence. An ordinary thrown error or invalid return value
invokes `fail()` with a bounded error description and `retryDelayMs` (default
1000). The queue's maxAttempts controls eventual dead lettering. Retried
external effects must be idempotent. An error explicitly reporting an uncertain
commit/checkpoint outcome stops the runner instead, including causes nested in
Error or AggregateError. Unreadable outcome fields fail closed.

`pollIntervalMs` controls empty-queue polling (default 1000). Each successful
handler also yields to the event loop, so a continuously nonempty queue cannot
starve cancellation and heartbeat timers with immediate Promise chains.

The runner performs one bounded `reapExpired()` before claiming, then one per
`reapIntervalMs` (default 30000). `reapLimit` defaults to 100 and is bounded to
1000. This includes jobs whose worker died on its final attempt. Reaping does
not run handlers, and full history is never retained by the runner.

With `stopWhenIdle: true`, each slot exits when its claim finds no runnable job.
The last slot wakes and joins the reaper before `done` resolves. This is a
finite drain of currently runnable work, not a global producer barrier: future
scheduled jobs, delayed retries, and jobs arriving after idle observations are
left for a later run. The startup expiry sweep remains bounded, not a promise
to reap the entire history. Continuous polling is the default.

## Acknowledged lease deadlines

The default lease is 30000 ms. `heartbeatMs` defaults to one third of the lease
and must be no greater than one third. Renewal is single-flight per job and
is joined before completion or failure. A pending renewal is never authority
to keep working past the last acknowledged deadline.

Each handler has an independent expiry alarm, not just the renewal loop. It
aborts the handler even when renewal is stuck in a transaction or awaiting a
checkpoint receipt. A late successful renewal does not resurrect an expired
handler. Queue, id, token, owner and attempt must still match when adopting a
renewal receipt. A delayed claim receipt cannot start an already-expired handler.

The worker compares both the saved absolute deadline and a monotonic budget
starting before the claim/renewal request. A backward wall-clock jump or a slow
acknowledgement cannot manufacture a new full lease budget. This is conservative:
long admission waits can cause a still-persisted lease to be abandoned. Choose
lease budgets longer than expected admission, SQL and checkpoint latency.

`clock` defaults to Date.now and must agree with the queue's wall clock and
other workers. Tests using an injected queue clock must pass that same clock
to the worker. Invalid clocks stop the runner. This is not a distributed-clock
guarantee. `context.checkpoint()` checks expiry synchronously and throws the
cancellation reason, even when CPU work or immediate promises starve timers.
JavaScript that never yields or checks cancellation cannot be forcibly stopped.

Known lease loss or conservative expiry aborts and drains that handler without
writing completion or failure over another owner's job. Lease storage checks
remain authoritative; local timers and external side effects are not atomic.

## Atomic application completion

Return `{ result?, apply }` to commit application SQL and job completion together.
Do parsing, computation and external I/O in the handler; keep apply limited to
SQL on its supplied transaction. The runner captures the callback and result
before waiting for admission and calls the queue's fenced `completeWith()`.

```ts
const atomicWorker = DurableJobWorker.start(jobs, async (lease, context) => {
  context.checkpoint();
  const computed = lease.payload.toUpperCase();
  return {
    result: 'indexed',
    apply: async (tx, applying) => {
      applying.checkpoint();
      // Application schema (indexed_documents) must already exist.
      await tx.execute('INSERT INTO indexed_documents(job_id, body) VALUES (?, ?)',
        [lease.id, computed]);
    },
  };
}, { owner: 'indexer-2', stopWhenIdle: true });
await atomicWorker.done;
```

The lease is checked before and after apply, and expiry alarms remain active
while apply is running. SQL error, application failure, cancellation or detected
expiry rolls back the application effects and completion together. Every SQL
promise must be awaited; do not retain tx or perform external side effects in
apply. Do not call another method on the owning job queue from inside apply.

Application/SQL failures from this completion phase stop the runner rather than
blindly rerunning computation. An abort interrupting apply joins rollback but
rejects `done` as a completion failure; it is not reported as a successful
cancellation/requeue. A detected lease-loss error can instead retire that claim.
A lost commit/checkpoint receipt preserves the unknown outcome and is never
converted to `fail()`. Reconcile before restarting.

## Shutdown and failure

`stop()` closes new claim admission and drains admitted work normally. Pending
claims may still return and run unexpired handlers. Heartbeats continue until
those handlers finish. Repeated stop calls return the same `done` promise.

`stop({ abort: true, reason })` additionally signals all active handlers. A
pending claim that returns after abort is failed without invoking its handler,
provided its lease is still valid. A cancelled active handler is failed only
after its callback and current heartbeat have finished. Cancellation consumes
the already-recorded delivery attempt and can dead-letter an exhausted job.
Graceful draining can escalate to abort but never switches back. SQL completion
already in flight is joined, not undone by pretending its receipt was cancelled.

The optional startup AbortSignal has abort-stop semantics. An already-aborted
signal runs no SQL. Stop wakes idle polling, removes its signal listener and
joins every owned operation before settling. It cannot forcibly terminate a
handler that ignores its signal. A handler may request `worker.stop()`, but must
not await that promise inside itself: it would be waiting for its own return.

Storage errors other than positively identified lease loss stop the entire
runner, abort sibling handlers, and reject `done` with DurableJobWorkerError.
Its `phase` identifies the failed operation and `cause` preserves the error.
No claim, renewal, completion, failure, or checkpoint is blindly retried. In
particular, a lost completion acknowledgement does not become a job failure or
permission to replay the handler. Already-admitted SQL still drains; new
mutations stop. Reconcile the authoritative store and checkpoint state before
restarting. Closing the database before joining the runner is unsupported.

`stats` is a frozen local runtime snapshot: active jobs, pending claims,
acknowledged completions/failures/cancellations, lease losses or conservative
expiry, renewals and reaped counts. These are not SQL state counts or a health
certificate. A commit that happened but was not acknowledged is not counted as
a confirmed success. Use `jobs.stats()` for actual persisted job states.

## Validation

```sh
node --experimental-loader=./packages/sdk/tests/helpers/source-loader.mjs \
  --test packages/sdk/tests/durable-job-worker.test.mjs packages/sdk/tests/durable-jobs-thread.test.mjs
```

The tests run the production runner and job SQL against actual Node SQLite.
They exercise overlapping handlers, acknowledged lease deadlines, atomic SQL
completion, shutdown, bounded recovery and uncertain storage outcomes. Thread
tests use four independent SQLite connections for managed consumers and force
termination after claims and during application transactions. They are not
FrankenSQLite native/WASM, browser durability, or power-loss tests.
