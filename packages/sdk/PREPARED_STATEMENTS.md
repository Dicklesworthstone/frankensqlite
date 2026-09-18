# Bounded prepared-statement ownership

The worker now bounds retained prepared statements independently of in-flight
requests. Completing a `prepare()` request no longer returns its retained
resource reservation. A new default worker permits 256 retained or currently
preparing handles and 16 MiB of accounted statement metadata.

```ts
const db = await FrankenDB.open({
  preparedStatementLimits: { maxStatements: 64, maxBytes: 4 * 1024 * 1024 },
});
console.log(db.preparedStatementLimits); // The acknowledged, immutable policy.
const statement = await db.prepare("SELECT name FROM users WHERE id = ?");
try {
  console.log((await statement.query([7])).rows);
} finally {
  await statement.finalize();
}
await db.close();
```

`FrankenDBQueue.open()` accepts the same database option. A transaction-owned
statement is still finalized when its scope ends. A child that cannot prepare
another handle can roll back without releasing its parent's handles. A failed
prepare inside a managed scope follows the usual scope-failure rules; resource
pressure does not authorize automatic replay of application SQL.

## Admission and recovery

A full pool rejects `prepare()` with `ERR_FSQLITE_STATEMENT_LIMIT`. Existing
handles remain valid: the worker never silently evicts them. Await finalization
of an unused handle, then retry preparation. There is no unbounded waiter queue.
A statement that individually exceeds the byte policy is rejected with
`ERR_FSQLITE_STATEMENT_TOO_LARGE`. Request-queue limits still apply separately;
await finalization before issuing more work when the request queue is also full.

Requested SQL is charged before parameter parsing or native preparation.
Binding-slot metadata and returned SQL/column names are charged before the
handle is published. Metadata extraction, capacity, cancellation or transport
failure cleans up an unpublished candidate exactly once. Finalization failure
uses the existing fatal connection contract instead of allowing more SQL on
uncertain resource state. Successful transaction completion, rollback, database
replacement and close release their owned handles. Failed replacement preserves
the old session's policy and handles.

The accounting formula is 256 bytes per handle, two bytes per UTF-16 code unit
of each requested/core SQL string, 16 bytes per parameter slot (including
numbered holes), and 16 plus UTF-16 bytes per column name. This is a deterministic
accounting policy, **not a heap/RSS or WASM-memory measurement**. It does not
measure native query plans, result sets, retained application bindings or core
execution memory. Unmanaged core handles outside this worker are not covered.
Ephemeral preparation inside one bulk request is not a retained public handle.

## Policy acknowledgement and host ceilings

`maxStatements` must be an integer in 1..4096; `maxBytes` must be an integer in
256..1073741824. Options are captured and validated before constructing a worker
or transferring an initialization image. Unknown policy fields are not sent.

The host's optional third constructor argument sets its ceilings:

```ts
const host = new WorkerConnectionHost(loader, requestLimits, {
  maxStatements: 1024,
  maxBytes: 64 * 1024 * 1024,
});
```

A client can ask for tighter limits, not weaken those ceilings. The effective
policy is the minimum of the requested values and the host's values. A default
worker therefore cannot be raised above its defaults through client options
alone. Reinitialization may change the requested policy only after the new
database has been staged and the previous session successfully retired.
`host.preparedStatements` exposes a frozen snapshot of handle count, accounted
bytes, limits and capacity refusals, including an in-flight prepare.

A new SDK accepts older workers when no policy was requested and reports
`db.preparedStatementLimits === null`; it does not invent an acknowledgement.
Explicitly requesting a policy from an older worker that omits it fails opening
with `ERR_FSQLITE_STATEMENT_POLICY`. Malformed or weaker-than-requested policies
also fail. The receipt is an interoperability contract, not an authentication
boundary against a malicious replacement worker.

## Executable checks

```sh
node --experimental-loader=./packages/sdk/tests/helpers/source-loader.mjs \
  --test packages/worker/tests/statement-budget.test.mjs \
  packages/sdk/tests/prepared-limits.test.mjs
```

These tests execute the actual SDK, worker client, request admission, host,
transaction and prepared-statement code. The integration suite uses real Node
worker-thread messages and the existing Node-SQLite reference adapter; special
malformed-receipt probes use a custom in-process transport. This is not the
browser worker entrypoint, browser storage, or FrankenSQLite WASM/native engine.
Use Node 22.16+ and the repository's TypeScript development dependency; the
existing `FSQLITE_TYPESCRIPT_MODULE` override can select an installed compiler.
