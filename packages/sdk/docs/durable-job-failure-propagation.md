# Finish workflow branches that can no longer run

`DurableJobQueue.cancelBlocked(limit = 100)` explicitly cancels ready jobs in
this queue when at least one immutable prerequisite is `dead` or `cancelled`.
The normal all-parent claim gate still applies. Impossible descendants never
run a handler or consume an attempt. Their payloads, identities and dependency
edges remain available for inspection and exact enqueue deduplication.

```ts
// Failure remains retryable until fail() exhausts the parent's attempts.
await queue.fail(lease, 'permanent failure');
// Explicitly propagate only known terminal failure, with a mutation budget.
const cancelled = await queue.cancelBlocked(100);
```

A still-ready/retrying, leased, expired-but-unreaped, or completed parent is not
terminal failure evidence. A missing parent is not inferred to have failed;
its dependent remains blocked for explicit reconciliation. Use `reapExpired`
first to turn exhausted crashed leases into dead jobs. Unrelated branches,
completed children and leased work are not cancelled. Future-scheduled children
may be cancelled immediately once a known prerequisite can never complete.

## Bounded and recoverable propagation

Each call owns one transaction and cancels at most 1..1,000 jobs. It selects
identifier-only pages of at most 32 and rechecks the failed-parent predicate
in each conditional UPDATE. Subsequent pages can see parents cancelled earlier
in the same transaction, including descendants that sort before their parent.
No recursion or unbounded in-memory graph walk is used. Repeating a call resumes
remaining work and never revives terminal identities.

The sweep is queue-scoped. A failed prerequisite may belong to another queue,
but cancellation never mutates that queue's children. Sweep each affected queue
under its own policy. A full limit can leave more descendants pending. There is
no atomic, unlimited cancellation transaction across all queues or databases.

The existing main-schema dependency unit is checked before and after a sweep.
Missing/incompatible dependency storage rejects; it is not re-created by this
operation. All reads and writes address main explicitly. TEMP and attached
shadows do not replace prerequisite state. Affected-row and retained-state
checks reject ignored or inconsistent writes. SQL errors, failed COMMIT and an
enclosing transaction's rollback undo the complete sweep, not just its last row.

A lost reply after COMMIT can mean cancellation succeeded. Inspect retained
states or repeat the bounded sweep on the same database. A returned count is
only the transitions committed by that invocation, not a global workflow
receipt. Nested results remain provisional. Snapshot-backed owners still need
genuine same-database storage confirmation. No callback retry, checkpoint,
external side effect or new global writer lock is introduced.

Cancelled descendants retain a fixed `lastError` explaining prerequisite
failure. The original failure remains on the failed ancestor; use
`dependencies(id)` to inspect the path. This is not a garbage collector,
compensation mechanism, arbitrary workflow-abort API, or a defense against
trusted code forging terminal states/removing database guards. Candidate scans
and indexed prerequisite checks still cost work; the mutation/page bounds do
not imply a bound on engine memory, scan cost or transaction latency.

## Executed verification

```sh
node --experimental-transform-types \
  --experimental-loader=./packages/sdk/tests/helpers/production-source-loader.mjs \
  --test packages/sdk/tests/durable-job-failure-propagation.test.mjs
```

The 44-case suite executes the actual queue on Node 22.16.0 / SQLite 3.49.1.
It covers all three encodings, bounded reversed chains and joins, retryable
parents, queue isolation, deferred commit failure, false write counts, ignored
updates, nested rollback, lost replies, independent WAL readers and competing
sweep owners. A 65-job case observes pages of 32, 32 and 1 without payload reads.
Eight IPC-confirmed SIGKILL cuts run after first/last cancellation and before/
after COMMIT under WAL/DELETE; watchdog termination fails the test. Reopened
files recover all-or-none propagation. Three initial cases fail on the original
queue without this capability. Strict TypeScript checks both actual queue/worker
sources, without dependency declarations or job-backend substitutes.

These are SDK-component/reference-SQL tests, not full FrankenDB packaging,
FrankenSQLite Rust/WASM/MVCC, browser persistence, or physical power-loss proof.

## Supervised failure propagation

The existing `DurableJobWorker` can opt in with `cancelBlockedJobs: true`:

```ts
const worker = DurableJobWorker.start(queue, handleJob, {
  owner: 'workflow-worker',
  cancelBlockedJobs: true,
  reapLimit: 100,
  stopWhenIdle: true,
});
await worker.done;
console.log(worker.stats.blockedCancellations);
```

The default is false, preserving existing behavior. Opt-in requires the supplied
queue adapter to implement `cancelBlocked`; unsupported adapters reject at start,
not after silently losing cleanup. The policy is copied once before asynchronous
startup and is not controlled by job payloads or handler results.

Sweeps share the existing supervisor: after initial and periodic expiry recovery,
and after an empty claim. `reapLimit` bounds each sweep. If an idle sweep makes
progress, the worker yields and checks again, so `stopWhenIdle` can finish chains
longer than one batch without claiming their cancelled nodes. Healthy branches
continue through the ordinary handler/continuation path. Retryable parents do
not cancel descendants. Periodic recovery can cancel impossible branches while
unrelated handlers are running. Each queue still needs its own supervisor/policy.

`blockedCancellations` counts acknowledged transitions by this worker, separately
from handler failures and cooperative handler cancellations. Invalid counts or
throwing/uncertain cleanup stop the worker with phase `cancel-blocked`. No cleanup
retry, additional claim, or inferred success is admitted after that failure.
Active handlers are signalled and joined before `done` rejects. A fresh owner must
reconcile committed state and storage before restart; lost acknowledgements can
leave cancelled jobs even when the failed worker counted zero.

Graceful or aborting stop joins a sweep already admitted to the transaction
owner. Such a sweep may commit all of its bounded cancellations; cancellation
is not proof of rollback. Stop prevents new sweep admission. No extra daemon,
interval, global writer lock, or statement queue is added by this integration.

The combined targeted run passes 143 tests: 44 new queue cases, 26 new production
worker cases, and 73 existing callback/continuation/worker regressions. Three
selected worker cases fail against the queue-only increment without supervisor
integration. Tests exercise consumed healthy/failing branches, startup and
periodic recovery, reverse chains beyond a batch, default/legacy behavior, input
ownership, shutdown joins, malformed acknowledgement counts, lost commit replies
with file reopen, and sibling cancellation. The combined run includes the eight
new queue SIGKILL cuts and eight existing continuation cuts rerun unchanged.
The 26 worker cases add no further process-kill scenarios. Strict TypeScript
checking uses the actual queue and worker source, without declaration substitutes.
These remain reference-SQL SDK-component results, not native engine certification.
