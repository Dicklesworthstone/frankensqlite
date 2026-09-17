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

Later writes on the original database do not appear in these replicas. All
replicas import identical, independently owned bytes. `snapshot` exposes a
frozen `{ sha256, byteLength }` identity; hashing is identity/corruption evidence,
not authentication. The caller's image is neither transferred nor detached.
A secure-context Web Crypto implementation is required to hash the image.

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
controlled worker delivery. They are not a FrankenSQLite WASM/browser certificate
or a throughput benchmark. The browser/WASM and wider pooling acceptance on
`bd-36fvl` remain separate gates; GitHub Actions remains off.
