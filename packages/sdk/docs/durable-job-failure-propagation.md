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
