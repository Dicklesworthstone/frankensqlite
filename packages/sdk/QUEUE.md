# Queuing independent callers

`FrankenDBQueue` is an opt-in, bounded FIFO of **whole transactions** on one
privately owned worker connection. Independent components can submit callbacks
at the same time without interleaving their transaction boundaries or receiving
ownership errors merely because another accepted callback is awaiting work.
The next job starts only after its predecessor's callback, admitted SQL,
prepared-statement cleanup, and commit or rollback have settled.

```ts
import { FrankenDBQueue } from "@frankensqlite/sdk";

const queue = await FrankenDBQueue.open(
  { dbName: "queued-notes", persistence: "indexeddb-snapshot" },
  { maxPendingJobs: 32 },
);
await queue.transaction(tx => tx.execute(
  "CREATE TABLE IF NOT EXISTS notes(id INTEGER PRIMARY KEY, body TEXT)",
));

// These independent transactions are admitted in this order.
const first = queue.transaction(tx => tx.execute(
  "INSERT INTO notes(body) VALUES (:body)", { body: "First" },
));
const second = queue.transaction(async tx => {
  const statement = await tx.prepare<{ id: number; body: string }>(
    "SELECT id, body FROM notes ORDER BY id",
  );
  return statement.all(); // Includes the first job's committed insert.
});
await first;
console.log(await second);

// Explicit persistence barrier, outside any transaction callback.
const saved = await queue.checkpoint();
console.log(saved.revision, queue.stats.pendingJobs);
await queue.close();
```

The database-open options are the same as `FrankenDB.open()`. Omit persistence
or select `memory` for an unsaved connection. The queue deliberately does not
expose its raw database: use the callback's `tx`, including `tx.prepare()`,
`tx.executeMany()`, `tx.executeStream()` and nested `tx.transaction()`.
Existing `FrankenDB` ownership behavior and native concurrent-writer defaults
are unchanged. This is **not multi-worker connection pooling**: independently
opened memory or snapshot sessions do not share live SQL state.

## Admission, cancellation and deadlines

`maxPendingJobs` counts the active job plus all waiting jobs. It defaults to 64
and accepts integers in 1..4096. A full queue promptly rejects with
`ERR_FSQLITE_JOB_QUEUE_FULL` (`transient: true`); that job's callback and SQL
never ran. There is no second queue of overflow waiters. Await accepted work
before retrying. The scheduler never retries callbacks automatically: external
callback side effects may not be safe to repeat. FIFO ordering applies to
accepted jobs, not to the order in which rejected callers retry admission.

```ts
const controller = new AbortController();
const result = queue.transaction(async tx => {
  await tx.execute("UPDATE notes SET body = ? WHERE id = ?", ["Updated", 1]);
}, { signal: controller.signal, waitTimeoutMs: 2_000 });
// A UI cancellation action may call controller.abort(reason).
await result;
```

A pre-aborted signal rejects admission. Aborting a waiting job removes it,
releases its reservation, and rejects with `ERR_FSQLITE_JOB_CANCELLED` and the
exact local abort reason as `cause`; no `BEGIN` or callback ran. `waitTimeoutMs`
is an optional integer in 1..2147483647. It is a **start deadline**, checked
again before dispatch even when the event loop delays timer delivery. Expired
waiting jobs reject with `ERR_FSQLITE_JOB_WAIT_TIMEOUT`, without executing SQL.
It never times out a transaction that has already started.

An active transaction uses the existing cooperative cancellation contract:
its callback, in-flight SQL and rollback must finish before the slot is
released. An abort after outer `COMMIT` dispatch cannot relabel a committed
success as cancelled. A callback or core operation that never settles can
therefore block later work and close; this API promises no wall-clock execution
interrupt. Pass the signal to the callback's own asynchronous I/O as well.

## Checkpoint, export and close ordering

`queue.checkpoint(options?)` and `queue.export(options?)` are FIFO jobs using
the same count limit and start-deadline rules. They run **outside SQL
transactions**, after prior jobs settle and before later jobs begin. Export
returns a portable image; checkpoint explicitly publishes an
`indexeddb-snapshot` revision and waits for the existing store's publication
acknowledgement. Memory-mode checkpoints still reject. Checkpoint failure does
not turn committed SQL into a rollback or discard the in-memory data.

For these image operations, `signal` only cancels **waiting** work. Once export
or checkpoint has started, its real outcome is authoritative: a late abort
cannot safely establish that snapshot publication did not happen. The queue
retains its slot until the actual result arrives. `snapshotRevision` continues
to mean the last loaded or published revision, not that all current writes
have been saved. Snapshot conflict, quota and corruption checks remain intact.

`close()` immediately stops new admission, drains all accepted jobs, and then
closes the connection. Repeated calls share the same promise, including a
close failure. Close does not checkpoint automatically and does not implicitly
cancel waiting jobs. A failed job is reported by its own promise; it does not
prevent later independent jobs or a checkpoint from running. A barrier waits
for predecessors to **settle**, not for every predecessor to succeed. Group
all-or-nothing application work inside one transaction rather than assuming
several queued transactions form one atomic unit. `Promise.all()` can reject
while other accepted jobs are still running; observe every submitted promise.

**Do not await another job on the same queue, an export/checkpoint, or close
from inside its active callback.** Such work is waiting for that callback to
finish and would deadlock. Use `tx.transaction()` for nesting and perform
queue-level barriers and close from outside the callback. The scheduler does
not attempt to infer asynchronous caller ancestry. Queueing captures the
callback reference, not a deep copy of the objects it closes over; capture
immutable input before submitting work that must use a fixed input snapshot.

`queue.stats` is a frozen snapshot with `state`, `maxPendingJobs`, active,
waiting and pending counts, plus cumulative accepted, completed, failed,
cancelled, timed-out and rejected job counts. `cancelledJobs` and
`timedOutJobs` count accepted jobs removed **before starting**; active
transaction cancellations count as `failedJobs`. Pre-aborted or invalid
admission counts as `rejectedJobs`. Completed/failed counts also include
checkpoint and export jobs. State describes scheduler admission, not a
connection-health certificate. The count bound does not cap callback-captured
memory, query results, database size or engine memory. Existing per-request
count/byte limits remain independently enforced beneath this queue.

## Queue verification

The Node SQL-reference suite is separately executable from the repository root
with workspace development dependencies installed:

```sh
npm run test:queue --workspace @frankensqlite/sdk
npm run typecheck --workspace @frankensqlite/sdk
```

The loader only transpiles TypeScript and resolves the workspace worker alias;
it does not typecheck or replace Vitest or the core engine. An optional
`FSQLITE_TYPESCRIPT_MODULE` selects an installed TypeScript module. The tests
use production SDK/worker code with real native SQLite files, a native C-API
binding reference, controlled failure boundaries, and actual Node worker
threads. IndexedDB tests are explicitly labeled transactional **models**, not
browser storage tests. Real browser/FrankenSQLite WASM and broader release gates
remain separate; no shared-database pool or page-level persistence is certified
by this suite.

## Committed table subscriptions

`await queue.subscribe(tables, listener, options)` installs connection-local TEMP
triggers at an ordered queue boundary. The returned subscription observes only
**later successful transactions on this queue**. It does not replay existing
rows, poll other connections, observe snapshot-pool replicas, or deliver a native
row-level update log. Names identify ordinary persistent `main` tables, not SQL
fragments. Views, virtual tables, reserved names and competing application TEMP
triggers on watched tables are refused.

```ts
const subscription = await queue.subscribe(["items", "audit"], async change => {
  console.log(change.tables, change.firstSequence, change.lastSequence);
  const current = await queue.transaction(tx => tx.query("SELECT * FROM items"));
  render(current.rows);
});
subscription.done.catch(reportListenerFailure);
await queue.transaction(tx => tx.execute("INSERT INTO items(id) VALUES (?)", [1]));
// Later, for example when the view unmounts:
subscription.unsubscribe();
await subscription.done;
```

Every notification is a frozen `CommittedTableChange` with frozen `tables` and
BigInt `firstSequence`, `lastSequence`, and `commits`. Sequences are local to this
queue, advance only for committed transactions that dirty a watched table, and
are **not native MVCC commit IDs, checkpoint revisions or durable acknowledgments**.
The table list is the intersection with that subscription's canonical names.
A notification means rows were affected, not necessarily that their final values
differ; an UPDATE to the same value or insert-then-delete may still invalidate.
Reads, no-row writes and writes to unobserved tables do not notify.

Dirty bits participate in real SQL transactions and savepoints. Rollback removes
both earlier writes and their invalidations, including trigger effects and a
failed later streaming-import chunk. A released child remains provisional until
the outer COMMIT succeeds. Deferred-constraint or cancellation rollback emits no
notification. Once COMMIT dispatch makes cancellation too late, a successful
commit still notifies. Only `checkpoint()` publishes snapshot-mode data; a
notification is not a browser persistence guarantee.

### Delivery, coalescing and lifecycle

Listeners run in a later task, outside transaction ownership. They may submit
and await new queued work. An async listener never holds up the writer queue.
Each subscription has **at most one running callback and one coalesced pending
notification**. A slow listener receives the union of dirty tables, sequence
range and matching-commit count; it does not receive an unbounded list of every
commit. Sequence gaps may represent commits affecting only other subscribers.
This is an invalidation API, not a lossless audit/event log. Requerying observes
current state, which may be newer than the delivered range.

A thrown/rejected listener fails only that subscription. Its original error is
available as `failure`, `state` becomes `failed`, and `done` rejects. The already
committed writer result is not changed and other listeners continue. Attach
error handling to `done`. No automatic listener replay or retry occurs.

`unsubscribe()` is synchronous and idempotent: it prevents future deliveries,
drops the pending notification, and works even with a full SQL queue. `done`
waits for an already-running listener to settle. The optional `signal` cancels
waiting/active registration using the existing queue/transaction contract; once
registered, it stops the subscription. A registration whose COMMIT already
succeeded may return a stopped handle after a late abort, never a false rollback
claim. `waitTimeoutMs` is only the registration's start deadline.

`close()` stops subscriptions immediately and drains accepted SQL/image jobs as
usual. It does **not** await listener code: a listener might itself await close
or queued work. Use a subscription's `done` separately to join its callback.
Never await your own `done` from inside that callback.

Unneeded TEMP triggers are removed before the next accepted SQL/image/registration
job, or destroyed with the private connection at close. Unsubscribe never
creates a hidden queue of cleanup jobs. Until that next boundary, unused trigger
resources remain bounded. Schema changes to watched tables fail closed before
commit; unsubscribe and then enqueue the schema migration. Do not modify the
reserved `__fsqlite_watch_` instrumentation: it is internal state, not a security
boundary against application SQL.

`maxSubscriptions` (second `open` argument) defaults to 64 and accepts 1..1024.
It counts active subscriptions **and stopped callbacks still running**, so rapid
unsubscribe/resubscribe cannot accumulate unbounded suspended listeners. Their
combined watch set is limited to 64 distinct tables; duplicate subscribers reuse
the table's three triggers and single dirty bit. `stats` exposes subscriptions,
reservedSubscriptions, maxSubscriptions, pendingNotifications and activeListeners.
These bounds do not cap user callback allocations, result sizes or process memory.
No subscription means no journal SQL or extra transaction boundary.

### Pull-based consumption

`await queue.changes(tables, options)` returns a `TableChangeStream` implementing
`AsyncIterableIterator<CommittedTableChange>` over the same commit-only feed:

```ts
const controller = new AbortController();
const changes = await queue.changes(["items"], { signal: controller.signal });
for await (const change of changes) {
  await refreshView(change.tables);
  if (viewIsClosed()) break; // for-await calls return(), unsubscribing.
}
```

The iterator owns one coalesced unread record in addition to the underlying
subscription's bounded delivery state. Slow consumers do not block SQL or
accumulate a result per transaction. Only one unresolved `next()` is admitted;
another rejects with `ERR_FSQLITE_SUBSCRIPTION_NEXT_PENDING`. Await it instead
of building an unbounded list of pending reads. `return()` immediately discards
buffered notices, unsubscribes and resolves a waiting `next()` as done. `throw()`
retains the supplied error and rejects a waiting `next()`. Lifetime abort and
normal queue close end iteration, even when no table has changed. These are
**change invalidations, not streaming SQL rows**.

A worker crash or an unrecoverable transaction rollback fails active
subscriptions and rejects waiting iterators without requiring a follow-up SQL
request. Original connection and listener errors are retained when both fail.
There is no transparent reconnection or replay. Queue opening also rejects a
worker that became ready and then failed before queue initialization completed.
A fault or close can discard undelivered notices of already committed work:
this local feed is not a durable log. Reopen/requery authoritative data to recover.

Register subscriptions/iterators outside transaction callbacks. Registration is
queued work, so awaiting it from a callback that already owns the same queue
would wait on itself. Notification listeners themselves are outside that
ownership and may register new observers or await SQL. A synchronous callback
still runs on the application's JavaScript thread; it has no database queue
reservation, but expensive callback code can block that thread.

### Executable subscription checks

With the repository's TypeScript dependency installed, run from its root:

```sh
node --loader ./packages/sdk/tests/helpers/source-loader.mjs --test \
  packages/sdk/tests/subscriptions.test.mjs \
  packages/sdk/tests/subscriptions-thread.test.mjs
```

`FSQLITE_TYPESCRIPT_MODULE` optionally selects an installed TypeScript module.
These tests exercise the production queue, client, transaction journal and worker
host against Node's SQLite reference. The real-thread suite imports production
`worker.ts` and uses its transferable binary-result path; its Web Worker event
bridge and native SQLite core remain explicitly test-only. This is not a browser,
FrankenSQLite WASM, cross-connection feed or release certificate. The exact core's
TEMP-trigger and schema behavior still needs browser/WASM validation.

## Demand-driven live queries

`watchQuery(queue, sql, options)` registers a table subscription and returns a
`LiveQuery<Row>` async iterator of complete `QueryResult<Row>` snapshots. It is
useful for lists, aggregates and views that should refresh after local commits:

```ts
import { watchQuery } from "@frankensqlite/sdk";

const controller = new AbortController();
const live = await watchQuery<{ id: number; value: string }>(queue,
  "SELECT id, value FROM items WHERE id >= ? ORDER BY id",
  { tables: ["items"], params: [1], signal: controller.signal },
);
try {
  for await (const result of live) {
    render(result.rows);
    // A UI unmount action can call controller.abort().
  }
} finally {
  await live.return?.();
}
```

The dependency subscription is installed **before the initial read**, so a
commit between registration and first demand cannot fall into a read/subscribe
gap. The first `next()` queues the initial read. Later `next()` calls wait for a
matching committed invalidation, then queue a fresh SELECT. Explicit `tables`
must include every ordinary main table the query depends on, including tables
behind joins, views and subqueries. Dependencies are not inferred from SQL.
This observes only the owning queue's writes, not other connections or replicas.

There is no polling, timer-based query loop, or prefetch without demand. While
a consumer is busy with one result, commits coalesce into a sequence marker,
not a backlog of result sets. The iterator admits only one unresolved `next()`
and one active read. Each read runs as a complete managed queue transaction,
using the normal request admission, cancellation and rollback contracts.
`ERR_FSQLITE_LIVE_QUERY_NEXT_PENDING` rejects a second unresolved `next()`.

Results retain `rows`, `rowArrays`, column metadata and negotiated binary
transport. The frozen outer result additionally reports `throughSequence`,
the queue's watched-commit high-water captured inside that read's ownership.
Delayed invalidations already covered by this high-water do not cause redundant
reads. This is a queue-local BigInt marker, not a native snapshot ID or durable
checkpoint. Results are current at their read, not a promise to reproduce every
intermediate commit. Rows can change again before application code uses them.
No deep-equality check suppresses an unchanged result after a real invalidation.

Only a single top-level `SELECT` is accepted. Scripts, `WITH`, transaction
controls, `PRAGMA`, and DML/`RETURNING` are rejected before registration rather
than replayed automatically. Queries still need to parse and execute in the
core. Application-defined SELECT functions must be side-effect free; lexical
preflight is **not a security sandbox** or proof against side effects in an
extension. This is whole-result requerying, **not incremental query evaluation or
streaming rows from the SQL engine**. A large result still materializes fully.

SQL and positional/named parameters are captured before registration awaits.
Blob data is copied, preserving aliases without retaining caller-mutated bytes.
`maxInputBytes` defaults to 1 MiB and accepts 256 bytes..64 MiB; it accounts for
the SQL/request envelope, scalar values and entire blob backing buffers. Shared
backing buffers are refused. This per-watch input bound and the queue's existing
subscription/job bounds are not limits on result size, consumer allocations,
engine memory, or aggregate process heap. Captured request data is released
when a stopped read finishes. The returned row objects belong to the consumer;
mutating them cannot alter the retained query parameters.

`return()` or `for await` break stops notifications, cancels this iterator's
waiting/active read, and **awaits that read's actual settlement and rollback**.
`done` provides the same join boundary. Lifetime abort and normal queue close
end iteration; SQL, admission/deadline, rollback and worker failures reject the
pending read and `done`, and stop only this iterator unless the connection itself
is unusable. An unrelated SQL or rollback failure is not suppressed merely
because cancellation happened at the same time. There is no automatic retry of
a failed read. A pre-aborted signal rejects registration. `waitTimeoutMs` applies
to registration and each read's start deadline, not running SQL duration.
Cancellation is cooperative; one nonsettling core operation can delay shutdown.
As with subscriptions, register/consume outside a callback owning the same queue.

The live-query tests use the public SDK entrypoint with the production queue,
worker and journal, plus a SQLite reference core. The actual Node-thread cases
reuse `subscriptions-thread.test.mjs`, exercise binary transfer, 12,000 exact
rows, cancellation cleanup and an actual worker crash. Run them alongside the
subscription suite using the source loader:

```sh
node --loader ./packages/sdk/tests/helpers/source-loader.mjs --test \
  packages/sdk/tests/live-query.test.mjs \
  packages/sdk/tests/subscriptions-thread.test.mjs
```

This does not certify the FrankenSQLite WASM artifact or browser execution.

## Snapshot-consistent table streaming

`scanTable(queue, tableName, options)` streams individual rows from an ordinary
`main` table without issuing one unbounded full-table query:

```ts
import { scanTable } from "@frankensqlite/sdk";

const controller = new AbortController();
const scan = scanTable<{ id: bigint; value: string }>(queue, "items", {
  columns: ["id", "value"],
  batchSize: 256,
  signal: controller.signal,
});
try {
  for await (const row of scan) {
    await consumeRow(row);
  }
  await scan.done;
} finally {
  await scan.return();
}
```

Construction captures arguments but does not admit a job or execute SQL. The
first `next()` starts one managed transaction at its FIFO position. Schema
inspection and every page use that **same transaction snapshot**, rather than
reopening a transaction between pages. Independent connections can still write
according to their native storage/MVCC rules. Later jobs on this queue, including
checkpoints and exports, wait until the scan releases its snapshot. Never await
another job on this same queue from the row consumer; use another connection or
finish/return the scan first.

### Indexed continuation and bounds

Ordinary rowid tables use an unshadowed hidden rowid. `WITHOUT ROWID` tables use
their full primary key, with its actual collations and individual ASC/DESC
ordering. `reverse: true` reverses the complete order. Continuation keys stay
bound values; signed 64-bit rowids require no addition or lossy conversion.
Composite continuation is split into disjoint equality-prefix/range seeks,
including mixed sort directions. There is no `OFFSET` or broad OR continuation
filter. The reference-engine tests inspect actual continuation query plans for
index searches without temporary sorting; native-engine plan/performance
qualification remains separate.

`batchSize` defaults to 256 and accepts 1..4096. The reader retains at most one
page of projected rows, releases consumed rows, and does not fetch another page
until a `next()` needs it. At most one unresolved `next()` is accepted; overlapping
demand rejects with `ERR_FSQLITE_SCAN_NEXT_PENDING`, not another hidden queue.
A composite page can require up to one SQL query per primary-key column. An
exact-sized final page requires a subsequent empty probe to establish EOF.
The frozen `scan.stats` reports successful pages, page query count, rows fetched,
rows yielded, current buffered rows and maximum buffered rows. These are counts,
not a JavaScript-heap measurement.

This bounds result **row count**, not bytes or total memory. Each core query
still materializes its bounded page, and transport/projection may temporarily
hold additional representations. A single text/blob value can be large. Engine
page caches, fallback execution, retained snapshot history and consumer-retained
rows are not bounded by this API. The continuation key is copied separately
before rows are exposed, capped at 1 MiB, so mutating a yielded key blob cannot
change the next page. Table metadata is capped at 1024 columns and a WITHOUT
ROWID primary key at 16 columns.

Projection entries are plain validated column names; omitted projection includes
all visible columns, including generated columns. Hidden key columns are not
added to the returned rows. Tables with every hidden rowid alias shadowed reject.
Views, virtual/shadow tables, other schemas, filters, joins and arbitrary SQL
expressions are not accepted. Unsupported or malformed metadata fails closed;
there is no fallback to an unbounded query. This is restricted table streaming,
not an arbitrary SELECT/VDBE cursor or an SQL security sandbox. Existing SQL
functions used by generated columns remain the application's responsibility.

### Completion, cancellation and snapshot release

Drain iteration or explicitly `return()` it. `for await` calls `return()` on a
loop break. Explicit return is a deliberate successful stop; caller signal abort
and queue close instead reject unfinished scans with
`ERR_FSQLITE_SCAN_CANCELLED`. They never silently report a truncated export as
successful EOF. Already delivered rows are only a prefix until normal completion.
When every row has been delivered and the final transaction is settling, its
actual outcome remains authoritative, including late abort after COMMIT dispatch.

A stopped scan discards buffered rows and waits for admitted SQL plus rollback
and handle cleanup before `done` settles. Cancelling a queued scan removes it
without BEGIN. Queue close wakes active scans even when their consumers are idle,
then drains their cleanup and other accepted jobs; ordinary queued transaction
callbacks retain their existing drain contract. SQL/rollback/transport failures
reject `next()` and `done`, retain causes, and are never automatically replayed.
The caller's own output or effects from already consumed rows are not rolled back.

To prevent an abandoned iterator from retaining a snapshot indefinitely,
`idleTimeoutMs` defaults to 30,000 milliseconds. It measures waiting for consumer
activity, not time spent inside SQL. `0` explicitly disables this lease; otherwise
use an integer in 1..2147483647. Expiry rejects with
`ERR_FSQLITE_SCAN_IDLE_TIMEOUT`, never successful EOF, and releases the snapshot.
Each new row request renews the idle lease. `waitTimeoutMs` remains an optional
integer start deadline in 1..2147483647, not an active SQL timeout. Cancellation
cannot interrupt one core operation that never settles.

The focused source tests are runnable from the repository root with the existing
TypeScript loader (set `FSQLITE_TYPESCRIPT_MODULE` when using a global compiler):

```sh
node --loader ./packages/sdk/tests/helpers/source-loader.mjs --test \
  packages/sdk/tests/table-scan.test.mjs \
  packages/sdk/tests/subscriptions-thread.test.mjs
```

These exercise production SDK/worker source against real SQLite reference files,
including actual Node worker threads and external WAL writers. They are not
FrankenSQLite WASM/browser execution, native MVCC/throughput certification, or
the full arbitrary-query streaming acceptance of `bd-o0s0h`.
