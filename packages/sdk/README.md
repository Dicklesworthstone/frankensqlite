# `@frankensqlite/sdk`

`@frankensqlite/sdk` provides the async, worker-backed TypeScript client for
FrankenSQLite in browser environments.

Current behavior:

- `FrankenDB.open()` starts a dedicated module worker and initializes the WASM
  runtime through `@frankensqlite/worker`.
- `execute`, `executeBatch`, `executeMany`, `query`, `prepare`, `export`, and `transaction`
  are exposed as Promise-based APIs.
- Persistence is intentionally memory-first until OPFS and IndexedDB backends
  land. Passing `opfs` or `indexeddb` surfaces an explicit worker error.

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
