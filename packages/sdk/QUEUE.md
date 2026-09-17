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
