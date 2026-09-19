# Exclusive browser snapshot ownership

Snapshot compare-and-swap protects publication, not the SQL or external work
performed before publication. Two independently opened images can both claim
the same persisted job and run its handler before one loses its checkpoint CAS.
Use `snapshotOwnership: "exclusive"` to admit only one participating live
session for a named snapshot database.

```ts
import { FrankenDB } from '@frankensqlite/sdk';

const db = await FrankenDB.open({
  dbName: 'offline-jobs',
  persistence: 'opfs-snapshot', // indexeddb-snapshot also supported
  snapshotOwnership: 'exclusive',
});
// Open resolves only after the worker holds exclusive session ownership and
// has loaded/imported the stored image, not merely advertised a capability.
console.log(db.snapshotOwnership); // exclusive
await db.execute('CREATE TABLE IF NOT EXISTS notes(id INTEGER PRIMARY KEY, body TEXT)');
await db.transaction(async tx => {
  await tx.execute('INSERT INTO notes(body) VALUES (?)', ['Saved by one session']);
});
await db.checkpoint(); // SQL COMMIT alone does not persist a whole-image backend.
await db.close();      // Joins admitted operations and relinquishes ownership.
```

The same open option passes through `FrankenDBQueue.open(databaseOptions,
queueOptions)`. Combine it with `checkpointOnCommit: true` for durable-job
consumers, and join `DurableJobWorker.done` before closing the database queue.
This avoids competing live snapshot copies; it does not make external effects
exactly once or make a failed checkpoint successful.

## Admission and identity

The database worker acquires a Web Lock before opening the snapshot store,
loading its bytes, or constructing the core database. An exclusive lease
excludes both exclusive and shared sessions using this protocol. A conflicting
open rejects with `ERR_FSQLITE_SNAPSHOT_OWNED` before constructing the database;
it does not wait indefinitely or accumulate a secondary queue of open requests.
There is no lock stealing or timeout-based takeover.

Each backend/name pair has separate authority. Identically named OPFS and
IndexedDB databases remain different databases. Names are encoded without
merging distinct lone-surrogate strings. Session ownership uses a different
lock from OPFS's short publication lock, so checkpointing does not reacquire
its own lifetime lock.

An omitted policy acquires shared ownership when Web Locks is available,
preserving multiple independent-image sessions and their existing CAS checks.
A legacy default IndexedDB session may operate without Web Locks; it has no
ownership acknowledgement. Explicit `shared` and `exclusive` policies instead
require Web Locks and reject with `ERR_FSQLITE_SNAPSHOT_OWNERSHIP_UNAVAILABLE`
when the API or the worker's exact acknowledgement is missing. Invalid policy
values or ownership requested for a non-snapshot backend fail before public
SDK worker construction.

The SDK captures request properties once and requires an own-data-field
acknowledgement matching the requested policy. An older worker that silently
ignores the field, a downgrade to shared, an unexpected exclusive policy, or
ownership claimed for memory cannot expose a database handle. `db.snapshotOwnership`
is the last negotiated policy (`shared`, `exclusive`, or null), not a liveness
probe or evidence that a closed/crashed worker still owns anything.

## Lifetime, close and recovery

Ownership remains in the database worker while it executes SQL, exports,
checkpoints, or confirms an uncertain publication. Close stops admission, joins
already-admitted operations, retires statements and the core, then awaits lock
release. A queued close cannot admit a competitor while a prior save is stalled.
Closing does not implicitly save uncheckpointed SQL.

A direct host reinitialization of the same namespace and policy transfers the
existing lease without releasing it between sessions. Rejected replacement
keeps the old live database and its lease. Changing an exclusive/shared policy
in place can conflict with the current lease; close the old session before
opening under a different policy. Successful replacement with a different
namespace releases the old lease only after retiring its database.

A destructor failure does not establish that the core is quiescent. The host
therefore retains that lease until its worker realm is terminated, rather than
letting a second close on cleared fields release it. The normal SDK terminal
cleanup retires the worker. A custom in-process host/transport must provide an
equivalent lifetime; discarding a JavaScript reference is not worker termination.
Web Locks are released by the browser when their worker realm is destroyed.

Publication recovery and exclusive ownership are independent requirements.
`requireCheckpoint` still verifies the exact restored publication and parent
when replacing a failed worker. Supply `snapshotOwnership: "exclusive"` again
on that reopen; a recovery identity is not transferable ownership. First retire
the old owner. A lost checkpoint receipt still needs confirmation/reconciliation,
not SQL replay. Preserve/export unsaved data before closing when necessary.

## Boundaries

This is cooperative, origin/storage-context-local ownership. Every competing
application must use a participating worker. Older clients, raw snapshot-store
calls, direct storage mutation, and hostile same-origin scripts do not honor
this session protocol. Web Locks are not an authorization boundary. No claim
is made across origins, browser profiles, devices or machines.

Shared mode does not share live SQL data; it retains independent snapshot
sessions. Exclusive mode also does not turn snapshots into a page-level VFS.
No native Rust file-lock, MVCC, or concurrent-writer default is changed.

A process can die after an external side effect but before its job completion
is checkpointed. A later owner can then see the job again. Stable idempotency
keys and the durable queue's at-least-once rules remain necessary. Session
ownership has no wall-clock lease and is distinct from per-job expiring leases.

## Verification

```sh
node --experimental-loader=./packages/sdk/tests/helpers/source-loader.mjs \
  --test packages/worker/tests/snapshot-ownership.model.test.mjs \
  packages/worker/tests/snapshot-ownership.test.mjs \
  packages/sdk/tests/snapshot-ownership.test.mjs

node --test packages/worker/tests/snapshot-ownership.browser.test.mjs
```

The first group executes production SDK/host/storage/lease code against actual
Node SQLite images with deterministic Web Locks, OPFS and IndexedDB models.
The browser suite targets real Chromium Web Locks across dedicated workers,
including termination. Its navigation was blocked by administrator policy in
the implementation environment, so no passing browser verdict is claimed.
Neither group is FrankenSQLite WASM/native conformance or power-loss validation.
