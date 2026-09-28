# Durable all-parent job joins

`EnqueueJob.dependsOn` declares immutable prerequisite job identities on the same
SQL database. A dependent job is runnable only after **every** named parent is
`completed`. Ready, leased, dead, cancelled and missing parents do not satisfy
that condition. Scheduling, priority, attempts and lease fencing still apply.

```ts
const jobs = await DurableJobQueue.open(database, 'reports');
await jobs.enqueue({ id: 'extract', payload: 'source A' });
await jobs.enqueue({ id: 'analyze', payload: 'source B' });
await jobs.enqueue({
  id: 'publish', payload: 'combined report',
  dependsOn: [
    { queue: 'reports', id: 'extract' },
    { queue: 'reports', id: 'analyze' },
  ],
});
```

The existing `DurableJobWorker` needs no separate dependency scheduler: its normal
claim path excludes blocked jobs before applying the page limit. Blocked jobs
consume no attempts and cannot hide runnable lower-priority jobs. `stats().ready`
still includes blocked/scheduled jobs; `stats().available` excludes them.
`stopWhenIdle` means no runnable job at that observation, not that every job is
finished. `dependencies(id)` returns a frozen snapshot of prerequisite identities
and their current states, or null for a missing child. No parent payloads/results
are transferred by that inspection API.

## Publication, identity and recovery

The job and its requirements are inserted in the same transaction, including with
`enqueueWith` and `completeAndEnqueue`. A failure rolls back both. Existing child
requests deduplicate only when their complete prerequisite sets also match;
reordering the same set is allowed, adding/removing an edge is not. Completed,
cancelled and dead children are not revived. Lost commit responses are reconciled
with the same input identity rather than new IDs or repeated business callbacks.

Every prerequisite must already exist when a new child is inserted. New edges
therefore point backward through insertion order; existing jobs cannot acquire
new requirements. This prevents cycles through the public API without traversing
all existing jobs. A prerequisite may be a currently leased parent of the same
`completeAndEnqueue` transaction. Cross-queue joins are supported only within the
same database, not across independently imported snapshots or remote services.

Inputs are copied and validated before asynchronous admission. A child accepts at
most 128 distinct prerequisites; a continuation batch accepts at most 1,024 total
prerequisites in addition to its existing 128-job/4-MiB payload limits. Identities
are case-sensitive and the existing identifier limits apply. Canonical comparison
is independent of SQLite UTF-8/UTF-16 sort order. Self-dependencies, missing parents
and conflicting retries reject rather than partially installing the child.

## Storage enforcement and compatibility

The first updated queue open installs `main.__fsqlite_job_dependencies_v1` and
three main-schema triggers together. The WITHOUT ROWID edge key serves per-child
lookup; prerequisite lookup uses the job identity key. A BEFORE UPDATE trigger
rejects a transition to leased while any prerequisite is incomplete. Thus an older
client's unfiltered claim cannot bypass dependencies after installation, although
that client may stop on a blocked highest-priority job instead of skipping it.
Update/delete guards keep the recorded edge set immutable. Requirements are not
reclaimed automatically; this feature is not a job/history retention policy.

Repeated open verifies the exact installed schema unit. Partial or incompatible
storage rejects; it is not silently repaired. Main qualification prevents TEMP or
attached objects from replacing the gate or parent state. Trusted application SQL
still owns the database; removing guards or forging parent completion is outside
this API's trust boundary. No external anti-rollback or distributed authorization
is implied. Failed parents leave visible blocked work; applications can inspect
and explicitly cancel the child, rather than silently running it after failure.

No new global writer lock, retry loop or background task is added. Independent
owners retain their normal transaction conflict semantics. Prerequisite checks
add indexed lookups per candidate; there is no claim of constant latency or bounded
total database memory. The host still owns durable commit/checkpoint confirmation.

## Executed verification

```sh
node --experimental-transform-types \
  --experimental-loader=./packages/sdk/tests/helpers/production-source-loader.mjs \
  --test packages/sdk/tests/durable-job-dependencies.test.mjs \
  packages/sdk/tests/durable-job-callback-lifetime.test.mjs \
  packages/sdk/tests/durable-job-continuations.test.mjs \
  packages/sdk/tests/durable-job-worker-continuations.test.mjs
```

The combined run passes 114 tests: 41 new prerequisite tests and 73 unchanged
callback/continuation/worker regressions, zero failures or skips. Production queue
and worker modules execute without substituted job backends over Node 22.16.0 /
reference SQLite 3.49.1 transaction ownership. Tests cover cross-queue joins,
blocked priority, failed parents, exact deduplication, canonical Unicode identities,
all three SQLite encodings, schema isolation, immutable requirements, lost replies,
independent WAL claimants, and actual worker consumption.

Eight new child processes are SIGKILLed at IPC-confirmed cuts before/after the
first dependency insert and before/after COMMIT under WAL/DELETE journals. Reopened
files recover all-or-none job/edge/application-effect publication. Watchdog kills
fail the test. The eight existing continuation crash cases also rerun; they are
not new prerequisite crash scenarios. Strict TypeScript checks cover both actual
source modules. This does not qualify FrankenSQLite Rust/WASM/MVCC execution,
FrankenDB ownership/worker packaging, browser persistence, or physical power loss.

## Atomic workflow graphs

`queue.enqueueBatch(jobs)` publishes an entire bounded workflow in one transaction.
Each input supplies `queue`, `id`, `payload` and optional `dependsOn`. Queues share
this queue handle's database; the method does not open another connection. Parents
may occur later in the input. A bounded iterative topological pass determines
insertion order, while returned job results remain in the original input order.
References outside the batch must already exist. Every duplicate job must match
its original payload, schedule policy, attempts and complete dependency set.

```ts
await jobs.enqueueBatch([
  { queue: 'reports', id: 'publish', payload: 'combined report', dependsOn: [
    { queue: 'reports', id: 'extract' },
    { queue: 'reports', id: 'analyze' },
  ] },
  { queue: 'reports', id: 'analyze', payload: 'source B' },
  { queue: 'reports', id: 'extract', payload: 'source A' },
]);
```

A missing parent, conflicting existing node, statement error or failed commit
rolls back every newly inserted node and edge. Existing exact jobs are neither
revived nor rewritten. Retrying the same batch after a lost commit response
recovers individual node identities; there is no separate global workflow receipt
or automatic replay of external effects. Enclosing transaction results remain
provisional until its outer commit. The queue still needs real storage confirmation.

The existing `completeAndEnqueue` and worker `DurableJobCompletion.next` accept the
same forward-referenced subgraphs. Thus a handler can atomically finish its parent,
publish business effects, create parallel successors and register their all-parent
join. Cycles reject before callback SQL or lease mutation. Parent completion retains
its existing final lease fence, including after subgraph insertion. No committed
prefix is exposed to other owners.

Batches admit 1..128 jobs, 4 MiB of combined payload and 1,024 prerequisite edges,
with 128 edges per child. Duplicate identities and internal cycles are rejected
before SQL. Graph ordering uses only the captured bounded input, not a scan of all
historical jobs. Queue/job identities must be well-formed Unicode so distinct
JavaScript strings cannot collapse during SQL binding. Public-API dependencies
remain immutable; arbitrary direct SQL graph surgery is unsupported.

### Graph verification

Add `packages/sdk/tests/durable-job-graphs.test.mjs` to the command above. The final
combined run passes 153 tests: 41 dependency cases, 39 graph cases, and the earlier
73 callback/continuation/worker cases. Two graph regression cases fail against the
first prerequisite-only increment. Graph cases include reversed forward references,
cross-queue identities, a 128-node chain, exactly 1,024 edges, byte limits, cycles,
lost commit replies, input ownership, outer/deferred rollback, independent-reader
visibility and actual worker fan-out/fan-in in all three database encodings.

Eight additional IPC-confirmed SIGKILL cuts interrupt graph nodes, edges and commit
under WAL/DELETE. After reopening, all nodes and edges exist together or none do;
retrying never resets committed job identities. Watchdog terminations fail. The
full combined run therefore includes 24 process-kill scenarios: eight graph cuts,
eight prerequisite cuts, and eight prior continuation cuts. Strict TypeScript
checking covers both actual queue and worker modules. These are reference-SQL
SDK-component results, not native FrankenSQLite, browser or power-loss evidence.
