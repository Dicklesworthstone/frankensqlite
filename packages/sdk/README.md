# `@frankensqlite/sdk`

`@frankensqlite/sdk` provides the async, worker-backed TypeScript client for
FrankenSQLite in browser environments.

Current behavior:

- `FrankenDB.open()` starts a dedicated module worker and initializes the WASM
  runtime through `@frankensqlite/worker`.
- `execute`, `executeBatch`, `executeMany`, `query`, `prepare`, `export`, and `transaction`
  are exposed as Promise-based APIs.
- `indexeddb-snapshot` provides opt-in, explicit whole-database checkpoints.
  It is not a page-level VFS or automatic per-SQL-commit persistence.
- `memory` remains the default. Passing `opfs` or `indexeddb` still surfaces
  an explicit error; their page-level storage backends are not implemented.

## Example

```ts
import { FrankenDB } from "@frankensqlite/sdk";

const db = await FrankenDB.open({ persistence: "memory" });
await db.execute("CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT)");
await db.execute("INSERT INTO users(name) VALUES (?)", ["Ada"]);

const result = await db.query<{ id: number; name: string }>(
  "SELECT id, name FROM users ORDER BY id",
);

console.log(result.rows);
await db.close();
```

## Atomic bulk writes

`executeMany(sql, parameterSets)` sends one worker request and reuses one prepared
statement for all input rows. It returns `{ executions, changes,
changesPerExecution }`; the counts are direct affected-row counts, not counts of
trigger or cascading effects. Values remain bound parameters, including `bigint`,
`null`, and `Uint8Array` blobs.

```ts
const imported = await db.executeMany(
  "INSERT INTO users(id, name) VALUES (?, ?)",
  [[1, "Ada"], [2, "Grace"], [3, "Linus"]],
);
console.log(imported.executions, imported.changesPerExecution);

// Existing prepared handles also support atomic batches and remain reusable.
const update = await db.prepare("UPDATE users SET name = ? WHERE id = ?");
await update.executeMany([["Ada Lovelace", 1], ["Grace Hopper", 2]]);
await update.finalize();
```

Each batch uses a unique savepoint. Outside a transaction it commits on success;
inside `tx.executeMany(...)` or a transaction-owned prepared statement, it stays
part of the enclosing transaction. A failing row rolls back the complete batch,
including earlier executions and their transactional trigger effects. A
finalization or deferred-constraint failure also rolls back before rejecting.
`FrankenSQLiteError.batchIndex` identifies a failing parameter set (zero-based).
Boundary failures have no row index. The SQLite codes, original `cause`, and
`cleanupErrors` survive the worker boundary.

Like other scoped operations, a failed `tx.executeMany()` makes that managed
callback fail even when its rejection is caught. To discard one batch and
continue the parent, run it in `tx.transaction(child => child.executeMany(...))`
and catch the child's failure after rollback. `OR ROLLBACK` or a rollback-raising
trigger may abort the entire outer transaction; failed savepoint recovery makes
the host unusable (`ERR_FSQLITE_BULK_CONNECTION_UNUSABLE`), never silently reusable.

One request accepts at most `MAX_EXECUTE_MANY_ROWS` (10,000) parameter sets. Larger
inputs reject before posting/writing; they are never silently split into partial
commits. To import more rows atomically, issue sequential batches within one
managed transaction. Empty input performs no preparation or writes, but still
checks connection/handle ownership. The SQL must be one `INSERT`, `UPDATE`,
`DELETE`, `REPLACE`, or `WITH` DML statement with no returned columns. Scripts,
transaction-control statements, `SELECT`, and `RETURNING` reject; use the existing
query API for results. `executeBatch` remains the separate SQL-script API.

### Cancelling a bulk write

Database, transaction, and prepared `executeMany` calls accept an optional
`{ signal: AbortSignal }` argument:

```ts
const controller = new AbortController();
const pending = db.executeMany(
  "INSERT INTO users(id, name) VALUES (?, ?)",
  [[10, "Ada"], [11, "Grace"]],
  { signal: controller.signal },
);
// For example, a UI cancel action can call controller.abort().
const result = await pending;
```

A pre-aborted signal rejects before posting the batch. Otherwise cancellation
uses a targeted control message that can reach queued or active work without
waiting behind it. An accepted cancellation finishes the currently executing
core operation, rolls back the complete batch, and only then rejects with
`ERR_FSQLITE_BULK_CANCELLED`. It does not merely abandon the caller's promise.
The control acknowledgement alone is not proof of rollback; always await the
original batch promise. Ordinary transaction error rules still apply, so use a
child transaction to recover a cancelled batch without failing its parent.

Cancellation is cooperative, not an interrupt of an individual SQL statement.
Cancellable batches yield to worker tasks before preparing and every 128
executions so resolved-promise chains cannot starve cancellation messages.
There is no wall-clock cancellation bound for one long-running core operation.
Once savepoint `RELEASE` is dispatched, cancellation is too late: the real commit
result remains authoritative, rather than falsely reporting committed writes
as cancelled. A failed cancellation-message delivery likewise cannot establish
rollback. Cancellation rollback failure makes the connection unusable and
retains the original cause and cleanup errors. Abort listeners are removed when
the batch settles; aborting a finished operation does not affect later work.

## Explicit browser checkpoints

Use a stable name and explicitly await `checkpoint()` after committing SQL:

```ts
const db = await FrankenDB.open({
  dbName: "offline-notes",
  persistence: "indexeddb-snapshot",
});
await db.execute("CREATE TABLE IF NOT EXISTS notes(id INTEGER PRIMARY KEY, body TEXT)");
await db.transaction(async (tx) => {
  await tx.executeMany("INSERT INTO notes(body) VALUES (?)", [["First note"], ["Second note"]]);
});
const saved = await db.checkpoint();
console.log(saved.revision, saved.byteLength, saved.sha256);
await db.close();
// Opening the same name in the same origin restores the last checkpoint.
```

**Only a successful, awaited checkpoint acknowledges snapshot publication.**
SQL operations and SQL `COMMIT` still operate on the in-memory database.
`close()` does not checkpoint automatically. A crash, tab close, or worker
termination discards writes made since the last successful checkpoint. This
explicit mode must not be confused with automatic IndexedDB/OPFS page
persistence or native FrankenSQLite MVCC across browser tabs.

The worker holds its request queue through export, hashing, and an atomic
IndexedDB compare-and-swap transaction. The SDK resolves only after that
transaction completes, not when its `put()` request succeeds. Publication
requests `durability: "strict"` and rejects browsers that do not expose that
policy. The policy is still a browser durability hint: origin eviction, user
data clearing, private browsing, and platform failure remain possible. It is
not an unconditional power-loss or permanent-retention guarantee.

`db.persistence` reports the resolved mode. `db.snapshotRevision` is the last
loaded or successfully published revision, **not** an indication that every
current in-memory write is saved. A checkpoint returns `SnapshotMetadata`:
`revision`, `parentRevision`, `byteLength`, and `sha256`. No data is published
just by opening a new name. An optional initialization `snapshot` can seed an
absent name, but rejects with `ERR_FSQLITE_SNAPSHOT_EXISTS` if a checkpoint
already exists; it never silently overwrites a saved database.

### Competing tabs and failures

Separate workers execute SQL independently. If two sessions loaded the same
revision, only one can replace it. The loser receives
`ERR_FSQLITE_SNAPSHOT_CONFLICT`; its in-memory data and expected revision stay
unchanged. Reopen the current checkpoint and explicitly merge application
changes, or export the losing session for recovery. Do not blindly retry a
stale checkpoint or assume row-level write merging. Unrelated database names
use separate IndexedDB databases rather than a shared write-transaction lock.

Quota, export, and failed publication do not discard the session's in-memory
changes. The previous checkpoint remains available, and a retry after fixing
the failure uses the same expected revision. Random revision tokens also
prevent an old session from mistaking an evicted/recreated snapshot for its
original revision. Corrupt, wrong-identity, or unsupported stored envelopes
reject initialization instead of silently creating an empty database.

Checkpoint outside all transactions, including manually issued `BEGIN` and
`SAVEPOINT`. Managed-callback ownership rejects `db.checkpoint()` inside a
callback. For raw SQL transactions the worker probes an empty `BEGIN` and
`ROLLBACK` boundary using the existing core API; a failed `BEGIN` never grants
permission to roll back the caller's transaction. Failure to roll back that
empty probe makes the connection unusable and closes its resources.

Snapshots are bounded to 64 MiB, use an exact-sized copy, and validate their
SQLite header/page alignment and SHA-256 before import. They are **not
encrypted**, and the checksum is corruption detection, not authentication
against other code on the same origin. Whole-image export/import is O(database
size) work and needs additional memory; the image limit is not a bound on
query memory or on the browser's internal IndexedDB allocations. Use the
existing export API for portable backups. Page-level browser persistence,
cross-tab native MVCC, and automatic durability remain separate work.

### Verification commands and scope

The storage model has a dependency-free Node 22.16+ runner:

```sh
node --experimental-transform-types --test packages/worker/tests/snapshot-store.model.test.mjs
```

The actual Chromium storage gate uses the repository's TypeScript and
Playwright dependencies and is intentionally separate from the model:

```sh
node --test packages/worker/tests/snapshot-store.browser.test.mjs
```

`FSQLITE_CHROMIUM_PATH`, `FSQLITE_PLAYWRIGHT_MODULE`, and
`FSQLITE_TYPESCRIPT_MODULE` optionally select installed test tools. Neither
runner silently substitutes a model for a browser. The SQL checkpoint
integration target `packages/worker/tests/snapshot-checkpoint.model.test.mjs`
uses the production SDK/worker, a transactional IndexedDB model, and Node's
SQLite reference engine; run it with an ESM TypeScript resolver honoring the
workspace's `@frankensqlite/worker` alias. It is not a WASM certificate.

At implementation time, model and reference-SQL tests passed; the actual
Chromium local test page was blocked by the execution environment's managed
URL policy. No policy was changed. Real browser/WASM and Rust release gates
must still be executed before claiming browser persistence certification.

## Transactions and connection ownership

Use the callback's `tx` handle for every operation in a managed transaction:

```ts
await db.transaction(async (tx) => {
  await tx.execute("INSERT INTO users(name) VALUES (?)", ["Grace"]);
  const statement = await tx.prepare("SELECT id FROM users WHERE name = ?");
  const result = await statement.query(["Grace"]);
  console.log(result.rows);
});
```

Ownership starts before `BEGIN` and lasts until commit or rollback settles.
Calls through `db`, or statements prepared outside that callback, reject with
`ERR_FSQLITE_TRANSACTION_OWNERSHIP` during this interval. They are not queued:
queueing a mistakenly awaited `db.execute()` inside the callback would deadlock.
Independent database connections are unaffected. Close the database after the
transaction finishes, not from inside its callback.

Transaction handles and their statements cannot be used after the callback
finishes (`ERR_FSQLITE_TRANSACTION_CLOSED`). Transaction-owned prepared
statements are finalized automatically before commit or rollback. Explicit
`finalize()` is also supported and idempotent once admitted.

Await every operation. The SDK nevertheless drains already-admitted work
before commit and fails closed if any scoped SQL operation failed, even if its
rejection was ignored or caught. A callback failure rolls back; simultaneous
callback/rollback errors are preserved in an `AggregateError` with the original
error as `cause`. If rollback fails, the worker is disposed because its
transaction state is no longer trustworthy. Do not issue manual transaction
control SQL (`BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT`, or `RELEASE`) inside a
managed callback; its transaction boundaries belong to the SDK.

### Nested transactions

Use `tx.transaction()` for a child savepoint. Catch a child's failure after its
rollback completes to continue the parent without discarding successful siblings:

```ts
await db.transaction(async (parent) => {
  await parent.execute("INSERT INTO users(name) VALUES (?)", ["Ada"]);
  try {
    await parent.transaction(async (child) => {
      await child.execute("INSERT INTO users(name) VALUES (?)", ["Temporary"]);
      throw new Error("Discard this child only");
    });
  } catch {
    // Ada remains in the parent; Temporary was rolled back.
  }
  await parent.execute("INSERT INTO users(name) VALUES (?)", ["Grace"]);
});
```

Children use unique SDK-generated `SAVEPOINT` names. Success releases the
savepoint; failure performs `ROLLBACK TO` followed by `RELEASE`. A released
child is still part of its parent: an outer rollback also undoes released
children. Nesting can span multiple levels, but only the deepest active child
may issue operations. Await siblings sequentially, and use the child's handle
inside its callback, not the parent or a parent's prepared statement.

Always await each child. If a parent callback returns with an active child,
the SDK drains that child and rolls back the parent with
`ERR_FSQLITE_TRANSACTION_UNAWAITED`, rather than committing early. A failed
rollback-to or cleanup release makes the entire connection unusable; the SDK
does not release an unsuccessfully rolled-back child into its parent.

### Closing

`close()` stops admitting new requests, completes previously admitted worker
requests, and shares one completion promise across repeated calls. A worker
crash or disposal rejects pending and future calls rather than leaving promises
unsettled. Failed initialization also releases the worker's resources.

## Streaming imports

`executeStream(sql, rows, options)` consumes an `Iterable` or `AsyncIterable`
of positional parameter arrays. It can import more than 10,000 rows without
materializing the complete input or returning one counter per input row:

```ts
async function* importedRows() {
  for await (const record of inputRecords) {
    yield [record.id, record.name];
  }
}

const imported = await db.executeStream(
  "INSERT INTO users(id, name) VALUES (?, ?)",
  importedRows(),
  { batchSize: 256, maxBatchBytes: 1024 * 1024 },
);
console.log(imported.executions, imported.changes, imported.batches);
```

The **whole stream is one transaction**, not one commit per worker chunk.
An invalid row, producer error, SQL error, or failed iterator/statement cleanup
rolls back all earlier chunks. Deferred constraints can still reject the final
commit. The returned counts acknowledge a successful outer SQL commit; they do
not imply that an `indexeddb-snapshot` checkpoint has been saved. Await an
explicit `db.checkpoint()` afterward to publish a snapshot.

`tx.executeStream(...)` uses a child savepoint for the complete import. A parent
can catch its failure and keep its own work and successful siblings. A later
parent rollback also undoes a successful import. Always await the stream before
returning from the parent callback. The source must not use `db` or a parent
handle to operate on the same connection while the import owns it; those calls
are refused. Independent connections remain independent.

There is one SDK/worker prepared handle for the stream, with bounded chunks
posted sequentially. The next chunk is not prefetched while a database request
is pending. Rows and blob values are copied on receipt, so a producer can reuse
its scratch row and buffer on subsequent yields. `batchSize` defaults to 256
(maximum 10,000). `maxBatchBytes` defaults to 1 MiB (maximum 64 MiB) and counts
16 bytes per row, 16 per value, UTF-16 string bytes and blob bytes. One row must
fit the budget; at most one detached lookahead row is held alongside a batch.
This is **parameter-buffer accounting, not a database or process-memory cap**:
the engine's data, transaction state, transport copies and producer memory are
separate. The snapshot image limit remains 64 MiB.

Stream-stage failures are `FrankenStreamError` instances. `phase` identifies
input, source, preparation, execution or cleanup; `rowIndex` is the zero-based
global input index when known (not merely the position within a chunk). `cause`
preserves the original producer or `FrankenSQLiteError`, including its SQLite
details, and `cleanupErrors` retains secondary failures. Boundary failures such
as a rejected `BEGIN`, final `COMMIT`, or rollback use the existing transaction
error contract. Iterators are closed once on early exit, and statement handles
are finalized before the enclosing transaction settles.

### Import progress and cancellation

```ts
const controller = new AbortController();
const pending = db.executeStream(
  "INSERT INTO users(id, name) VALUES (?, ?)",
  importedRows(),
  {
    signal: controller.signal,
    onProgress(progress) {
      // These rows have executed, but the import has NOT committed yet.
      console.log(progress.executions, progress.batches, progress.committed);
    },
  },
);
// A cancellation action may call controller.abort().
const committed = await pending;
```

`onProgress` receives a frozen snapshot after each successful chunk, always
with `committed: false`. The next source pull waits for an async callback to
finish. A thrown/rejected callback aborts the entire import. No progress event
claims that a SQL commit or browser checkpoint succeeded; use the final promise
and explicit checkpoint result for those acknowledgments.

An abort observed before the final transaction boundary rolls back the complete
stream, including earlier chunks. A cancellation arriving too late for one
chunk's savepoint release can still undo that chunk through the enclosing
transaction. Once the **outer COMMIT** (or the import's child-savepoint RELEASE)
has been dispatched, its actual result wins: a late signal cannot relabel a
committed success as cancelled. A genuine SQL failure is not hidden by a
coincident abort. `FrankenStreamError` uses
`ERR_FSQLITE_STREAM_CANCELLED` for cancellation observed by the importer; its
cause and cleanup failures remain available. Progress errors have phase
`progress`, and producer errors retain phase `source` and their original cause.

Cancellation is cooperative. The importer awaits an outstanding `next()`,
progress callback, iterator `return()`, or individual core SQL operation rather
than abandoning it. Pass the same signal to the producer's own I/O so a blocked
input can respond. A producer or callback that never settles can therefore
delay cancellation; no wall-clock interruption bound is promised. No additional
rows are consumed or written after cancellation is observed, and the connection
remains owned until cleanup and rollback settle.

## Bounded request admission

Each SDK client and each worker host independently limits ordinary outstanding
requests. Defaults are **128 active-plus-queued requests** and **128 MiB of
accounted request payload**. Admission refuses overload rather than creating
another unbounded queue of waiting promises. Await operations or use
`executeStream()` to feed a large input sequentially.

```ts
const db = await FrankenDB.open({
  requestLimits: {
    maxPendingRequests: 32,
    maxPendingBytes: 16 * 1024 * 1024,
  },
});
console.log(db.requestQueue.pendingRequests, db.requestQueue.pendingBytes);
```

Options are captured before worker creation. `maxPendingRequests` accepts
integers in 1..4096 and `maxPendingBytes` accepts 256 bytes..1 GiB. The frozen
`db.requestQueue` snapshot also reports the configured limits and cumulative
`rejectedRequests`. It describes this client's reservations, not engine memory
or the worker's independently configured budget. Custom worker hosts can set
their own limits via `new WorkerConnectionHost(loader, limits)` and inspect
`host.requestQueue`. An SDK setting cannot raise the receiver's limits.

`ERR_FSQLITE_QUEUE_FULL` is a transient admission refusal: **none of that
request's SQL ran**. Await previously admitted work before retrying. A request
too large even for an idle queue returns `ERR_FSQLITE_REQUEST_TOO_LARGE`; reduce
its size instead of retrying unchanged. Inside a managed transaction, a caught
admission error still causes that scope to drain and roll back, just like other
scoped operation failures. Retry the complete transaction or use a child scope
for recoverable work; do not assume earlier writes were committed.

Pre-IPC checks capture plain protocol fields and positional scalar arrays before
`postMessage`, excluding extra object properties and custom array iterators.
Accounting includes SQL and UTF-16 text, fixed message/array/value allowances,
and each distinct binary backing buffer's full capacity within a request, not
just the visible bytes of a small subarray. This avoids cloning a large backing
allocation disguised as a tiny view. Reservations last until settlement and
are returned on SQL errors, synchronous transport failures, crashes and disposal.

Close reserves a separate, deduplicated fence; cancellation control is not
trapped behind a full SQL queue. Already-admitted work settles before close
frees handles. A queue-refused statement `finalize()` remains retryable, and
transaction cleanup retains ownership of that statement. Once the worker's
close fence is admitted, later SQL and reinitialization are rejected.

These limits are **not a heap/RSS, result-size or database-memory guarantee**.
They do not bound engine transaction state, prepared handles retained by an
application, producer-owned memory, query results, or a custom sender's runtime
message queue before the host receives messages. The byte estimator is explicit
accounting rather than a JavaScript heap measurement. Stream `maxBatchBytes`
and request accounting have different envelope allowances; choose stream chunks
that fit both budgets. Independent connections still execute independently.

## Manual npm Release

For v0.2.0, release preparation and publication are manual. No GitHub Actions
run is required or used, and a green workflow is not release authorization.
Publish the browser packages in dependency order:

1. `@frankensqlite/core`
2. `@frankensqlite/worker`
3. `@frankensqlite/sdk`

Start from the exact, clean release commit. Build the worker-compatible core
archive and validate the worker and SDK against it, then publish the exact core
and worker versions before packaging the SDK. Inspect the SDK package without
publishing it, then create the release tarball in a dedicated artifact
directory:

```bash
npm pack --workspace @frankensqlite/sdk --dry-run
mkdir -p artifacts/npm
npm pack --workspace @frankensqlite/sdk --pack-destination artifacts/npm
```

The package's `prepack` script reruns typechecking, tests, and the TypeScript
build so the inspected or published package is built from the current source.
The pack listing must contain the expected `dist/` entries, `README.md`,
`LICENSE`, and `package.json`, and no source, test, or unrelated repository
files. Install and smoke-test the resulting
`artifacts/npm/frankensqlite-sdk-0.2.0.tgz`; that exact archive is the only SDK
payload eligible for publication. Publish only after
`@frankensqlite/core@0.2.0` and `@frankensqlite/worker@0.2.0` are available from
the registry. Do not substitute an older `dist/` tree or a package built from
a different commit.

After those checks and the project-wide release gates pass, publish the exact
tested archive under the staging dist-tag:

```bash
npm publish artifacts/npm/frankensqlite-sdk-0.2.0.tgz --access public --tag next
```

The manifest pins publication to the public npm registry with public access.
After all three packages have been verified from clean registry installs,
promote `latest` in dependency order: core, worker, then SDK.

## License

This package uses the repository's custom MIT License with OpenAI/Anthropic
Rider. It is not the SPDX `MIT` license. The complete, controlling terms are in
the package-local [`LICENSE`](LICENSE) file and are included in the npm tarball.
