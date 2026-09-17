# Parallel snapshot reads

`FrankenSnapshotPool` runs concurrent read queries on dedicated workers, each
importing the same copied SQLite database image. This is an immutable snapshot
read pool, **not** a pool of connections sharing a live writable database. Write
through `FrankenDB` / `FrankenDBQueue`, export a committed image, then open a pool:

```ts
import { FrankenSnapshotPool } from "@frankensqlite/sdk";

const image = await writer.export(); // Outside all SQL transactions.
const readers = await FrankenSnapshotPool.open(image, {
  workers: 2,
  maxPendingQueries: 64,
  maxPendingBytes: 32 * 1024 * 1024,
  resultEncoding: "auto",
});
const [totals, recent] = await Promise.all([
  readers.query<{ total: number }>("SELECT count(*) AS total FROM items"),
  readers.query("SELECT * FROM items ORDER BY id DESC LIMIT ?", [100]),
]);
console.log(totals.rows, recent.rows, readers.snapshot.sha256);
await readers.close();
```

Later writes on the original database do not appear until an explicit refresh.
All replicas import identical, independently owned bytes. `snapshot` exposes a
frozen `{ sha256, byteLength, generation }` identity; the generation starts at 1.
Every query result also has `result.snapshot`, identifying the generation that
actually executed that query, even if a refresh completed before the caller
examines the result. Hashing is identity/corruption evidence, not authentication.
The caller's image is neither transferred nor detached. A secure-context Web
Crypto implementation is required to hash it.

## Atomic refresh

```ts
// Finish the writer's SQL transaction before exporting. With FrankenDBQueue,
// export() is itself an ordered barrier after preceding queued transactions.
const nextImage = await writer.export();
const refresh = readers.refresh(nextImage);
const nextRead = readers.query("SELECT count(*) AS total FROM items");
const published = await refresh;
const result = await nextRead;
console.log(published.snapshot, result.snapshot); // Same generation.
for (const error of published.cleanupErrors) console.error(error);
```

`refresh(image)` reserves one **pool-wide FIFO barrier**. All earlier reads
finish on the old image. The pool then imports the copied replacement into new
workers and verifies every replica's read-only policy. Only after every replica
is ready does it publish the complete generation in one step. Queries admitted
after the barrier cannot run until it settles. There is no per-worker gradual
rollout, mixed generation, transparent query replay, or partial database merge.

If import/validation fails before publication, the old generation is retained
and staged workers are closed. Queries queued behind the failed refresh reject
with `ERR_FSQLITE_POOL_REFRESH_FAILED` **without running** rather than silently
serving old data. Subsequent, explicitly submitted queries may still read the
old snapshot, or the application can retry refresh with a valid image. A worker
crash instead makes the pool unusable; failed workers are not silently replaced.

A successful refresh returns `{ snapshot, cleanupErrors }`. If retiring an old
replica fails after publication, the new generation remains published and the
error is included in `cleanupErrors`; it does not falsely report a failed
refresh that callers might blindly retry. The returned object and error array
are frozen. A byte-identical refresh still advances the local generation.

Only one refresh can be pending; another rejects with
`ERR_FSQLITE_POOL_REFRESH_BUSY`. The barrier has its own single-image admission
slot, separate from query count/payload capacity. The replacement obeys the same
per-image and aggregate replica-input limits as open. During staging both old
and new generations coexist: their combined replicated image bytes can reach
256 MiB, plus the source copy, engine/import/cache/result memory. This is not a
heap bound. `stats.refreshing` and `pendingSnapshotBytes` expose the barrier and
owned replacement-image byte count, not total staging memory.

Refresh has no forced cancellation. Cancelling a read waiting behind it removes
only that read. Close drains any accepted refresh and then closes the published
generation too. A custom worker factory must return new dedicated workers for
each generation; reusing an earlier worker is rejected before reinitializing it.

Snapshot refresh does not publish IndexedDB data, make a writer commit durable,
or automatically observe external writes. Those boundaries remain the writer's
SQL commit and, for `indexeddb-snapshot`, its explicit checkpoint.

## Read-only contract

The pool verifies `PRAGMA query_only = ON` on every replica before returning it.
A core that ignores the pragma or does not acknowledge it makes initialization
fail. The pool exposes no connection, transaction, prepared handle or write API.
It accepts a single `SELECT` or `WITH` statement, and `EXPLAIN [QUERY PLAN]` of
those forms. The existing whole-statement preflight rejects scripts and manual
transaction controls. The engine query-only policy rejects write-containing
`WITH` statements. Column metadata, positional/named bindings and negotiated
binary result transport use the existing SDK/worker implementations.

PRAGMA, ATTACH/DETACH and maintenance operations are not accepted, even through
EXPLAIN: some pragmas take effect during preparation. Read-only table-valued
pragma functions may be used in SELECT where the core supports them. This is
not a sandbox for hostile SQL, untrusted extensions or application-supplied
workers; custom worker factories must return distinct, exclusively owned
workers implementing the normal core contract. Query-only admission is not a
native read-only file descriptor, nor does it alter native MVCC writer defaults.

## Capacity and ownership

`workers` defaults to 2 and accepts 1..8. Each image is limited to 64 MiB by the
existing snapshot validator, with a 128 MiB limit on `image bytes * workers`.
These are input-image limits, **not** heap/RSS limits: core database state,
import copies, caches, temporary query state, full query results and caller
retention can use additional memory. Results are still materialized; this is
not row streaming.

`maxPendingQueries` bounds active plus waiting queries (default 64, range
1..4096). `maxPendingBytes` bounds accounted input payload across them (default
128 MiB; same range/accounting as the SDK request budget). Overload rejects with
`ERR_FSQLITE_QUEUE_FULL`; an individually oversized request rejects with
`ERR_FSQLITE_REQUEST_TOO_LARGE`. Neither rejection runs SQL. There is no hidden
overflow queue and no automatic retry. Wait for admitted work before retrying.

SQL and binding values are captured at admission, not when a waiting job starts.
Blob storage is copied with alias preservation and charged by the complete
backing allocation. SharedArrayBuffer inputs are refused. Changing a caller's
object, array, blob or source image cannot change queued query arguments or a
replica. Reentrant binding getters cannot enqueue after close or evade budgets.

Queries start in FIFO order on the next available replica; completion order can
differ. A slow query holds only its own replica. Each worker receives at most one
pool query at a time. Frozen `stats` reports state, worker count, active/waiting/
pending queries, pending bytes and completed/failed/rejected query counts.

## Cancellation, deadlines and shutdown

```ts
const controller = new AbortController();
const pending = readers.query("SELECT * FROM items WHERE id > ?", [100], {
  signal: controller.signal,
  waitTimeoutMs: 1000,
});
// A UI cancellation action can call controller.abort(reason).
const result = await pending;
```

A waiting abort removes its job without running SQL. An active abort discards a
successful result **after** the underlying query settles, retaining capacity
until then. It does not interrupt an individual core operation. A simultaneous
SQL failure remains the reported error. `waitTimeoutMs` applies only until
start; a delayed timer cannot let an already-expired waiting job run. Zero is an
already-expired deadline. Cancelled/deadline-expired admitted jobs count as failed.

`close()` stops admission and drains all accepted queries before closing every
worker. Repeated closes share one promise. A worker crash fails waiting jobs,
rejects future admission, drains other active replicas and closes the pool;
there is no transparent query replay or replacement with a different image.
Initialization failure drains all opening attempts and closes every successfully
opened replica. Cleanup errors remain in an AggregateError rather than replacing
the original failure.

## Executed test boundary

Run the standalone Node reference target with the workspace TypeScript installed:

```sh
npm run test:snapshot-pool --workspace @frankensqlite/sdk
```

The tests use production SDK/worker source and real SQLite reference files with
both controlled delivery and actual Node worker threads. The real-thread cases
load the production `worker.ts` entry point and its transfer-list path, replacing
only the core with Node SQLite. A test-only function inside SQLite establishes
actual simultaneous execution on three threads before allowing any to finish.
Other cases check exact large results, cancellation, abrupt worker termination,
file integrity and generation refresh. The thread bridge translates an unexpected
Node exit into a browser-style error event; that is not browser crash evidence.
Named binding tests use a separate Python/system SQLite C-API oracle.

These are not a FrankenSQLite WASM/browser certificate or a throughput benchmark.
Read-only enforcement still needs validation against the exact shipped core
artifact. The browser/WASM and wider pooling acceptance on `bd-36fvl` remain
separate gates; GitHub Actions remains off.
