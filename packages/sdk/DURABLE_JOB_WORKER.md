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
const worker = DurableJobWorker.start(jobs, async (lease, { signal }) => {
  signal.throwIfAborted();
  const input = JSON.parse(lease.payload);
  // Await application work here. Pass signal to cooperative operations and use
  // lease.id as their idempotency key; external effects remain at least once.
  return JSON.stringify({ processed: input.id });
}, { owner: 'indexer-1', concurrency: 4 });

// Later, stop intake and join already-admitted claims, handlers and renewals.
await worker.stop();
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

A handler returns a string (up to 1 MiB UTF-8), null, or undefined. Successful
work is completed using its persisted ownership fence. A thrown error or
invalid return value invokes `fail()` with a bounded error description and
`retryDelayMs` (default 1000). The queue's maxAttempts controls eventual dead
lettering. Retried external effects must be idempotent.

The default lease is 30000 ms. `heartbeatMs` defaults to one third of the lease
and must be no greater than one third. Renewal is single-flight per job and
is joined before completion or failure. Lease loss aborts that handler's signal
and drains it without overwriting the replacement owner's job. Lease storage
checks remain authoritative; timers and external side effects are not atomic.

`pollIntervalMs` controls empty-queue polling (default 1000). Each successful
handler also yields to the event loop, so a continuously nonempty queue cannot
starve cancellation and heartbeat timers with immediate Promise chains.

The runner performs one bounded `reapExpired()` before claiming, then one per
`reapIntervalMs` (default 30000). `reapLimit` defaults to 100 and is bounded to
1000. This includes jobs whose worker died on its final attempt. Reaping does
not run handlers, and full history is never retained by the runner.

## Shutdown and failure

`stop()` closes new claim admission and drains admitted work normally. Pending
claims may still return and run their handlers. Heartbeats continue until those
handlers finish. Repeated stop calls return the same `done` promise.

`stop({ abort: true, reason })` additionally signals all active handlers. A
pending claim that returns after abort is failed without invoking its handler.
A cancelled active handler is failed only after its callback and current
heartbeat have finished. Cancellation consumes the already-recorded delivery
attempt and can dead-letter an exhausted job. Graceful draining can escalate
to abort but never switches back.

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
acknowledged completions/failures/cancellations, known lease losses, renewals and
reaped counts. These are not SQL state counts or a health certificate. A commit
that happened but was not acknowledged is not counted as a confirmed success.
Use `jobs.stats()` separately for actual persisted job states.

## Validation

```sh
node --experimental-loader=./packages/sdk/tests/helpers/source-loader.mjs \
  --test packages/sdk/tests/durable-job-worker.test.mjs
```

The tests run the production runner and job SQL against actual Node SQLite.
They exercise overlapping handlers, lease renewal and loss, pending claims,
shutdown, retry exhaustion, bounded recovery, and uncertain storage outcomes.
They are not FrankenSQLite native/WASM, browser durability, or power-loss tests.
