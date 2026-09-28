# Durable job transactions and callback ownership

`DurableJobQueue.enqueueWith()` couples application SQL to job publication.
`completeWith()` couples application SQL to a live, fenced job completion.
Their callback executors admit SQL only while the callback is active. Every
admitted `execute()` or `query()` settles before publication/commit, the final
completion lease check, or propagation of a callback failure for rollback.

A failed admitted statement aborts the operation even when application code
catches its rejection. A callback failure takes precedence over child failures,
but still waits for children to finish. Handles retained past callback completion
reject with `ERR_FSQLITE_JOB_SCOPE_ENDED` before reaching the SQL adapter; they
cannot be reused in the queue's postlude or another transaction. The first SQL
failure is retained, including falsy rejection values, not an unbounded error list.

Always await statements in application code. This drain is a correctness boundary,
not permission to create unlimited outstanding work: transaction ownership,
statement admission limits and execution ordering remain the database adapter's
responsibility. No additional SQL queue, writer lock, retry, timer or background
worker is introduced. A hung statement still requires the owner's cancellation
policy; the helper cannot abandon it safely.

Completion expiry is checked after draining SQL. If the lease expires during that
work, its application writes and completion both roll back. Duplicate enqueue
still skips application work. Completed or replaced leases cannot rerun completion
callbacks. A lost post-COMMIT response is uncertain, not proof of rollback; recover
from the retained job state, never rerun business work blindly.

Callbacks are trusted SQL composition, not a sandbox. They must not commit or roll
back the owner, change queue metadata, execute via another handle on the same
connection, reenter the queue, or perform external effects that SQL cannot undo.
The host must provide a transaction that settles after commit/rollback, including
any required same-database storage confirmation. Independent browser snapshots
are not a shared live queue. No native SQL registration or new schema is implied.

## Verification

From the repository root, using Node 22.16.0 or a compatible Node release:

```sh
node --experimental-transform-types --test \
  packages/sdk/tests/durable-job-callback-lifetime.test.mjs
```

The 30 tests execute the production queue with real reference SQLite statements,
transactions, constraints and savepoints. Explicit barriers delay actual SQL;
no SQL interpreter or queue implementation is substituted. The unchanged queue
fails 24 cases. Coverage includes execute/query RETURNING, callback and statement
failures, delayed lease expiry, late handles, duplicate and lost-commit responses,
UTF-8/UTF-16LE/UTF-16BE, and deferred foreign-key failures at COMMIT. Strict
TypeScript 5.8.3 checking covers the actual queue module, which has no imports.
This is SDK-component/reference-SQL evidence, not execution of FrankenSQLite
Rust/WASM/MVCC, the full worker package, browser durability or physical power loss.
