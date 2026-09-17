# Opt-in transaction conflict recovery

`FrankenDB.transactionWithRetry(work, options)` retries an entire managed
transaction, not an individual SQL statement. Every replay uses a new `BEGIN`,
new transaction id and fresh callback handles, after the previous attempt's
operations, prepared statements and acknowledged `ROLLBACK` finish. Ordinary
`transaction()` remains single-attempt.

```ts
const controller = new AbortController();
const value = await db.transactionWithRetry(async (tx, { attempt }) => {
  const result = await tx.query<{ value: number }>(
    "SELECT value FROM counters WHERE id = ?", [1],
  );
  const next = result.rows[0]!.value + 1;
  await tx.execute("UPDATE counters SET value = ? WHERE id = ?", [next, 1]);
  return { value: next, attempts: attempt };
}, { maxAttempts: 4, timeoutMs: 5000, signal: controller.signal });
```

Read-dependent writes must repeat their reads: retrying just the failed write
would reuse decisions made against the old snapshot. A child savepoint does
not establish a fresh outer snapshot. An uncaught child conflict therefore
restarts the complete outer callback; no retry method is exposed on a child.

## Replay contract

The callback can execute more than once. Put irreversible external effects
(sending mail, charging a card, publishing a message) after the returned
promise succeeds, or use application-level idempotency and a transactional
outbox. SQL rollback does not undo JavaScript or network effects. Recreate
iterators/streams inside the callback; never reuse an already-consumed source
or a transaction/prepared handle from a previous attempt. Return values from
failed attempts are discarded.

Only typed SQLite BUSY-family errors qualify: `SQLITE_BUSY`,
`SQLITE_BUSY_RECOVERY`, `SQLITE_BUSY_SNAPSHOT`, and `SQLITE_BUSY_TIMEOUT`, with
consistent numeric codes when present. All members of an aggregate failure
must qualify. Constraint errors, application errors, `SQLITE_LOCKED`, malformed
error trees, arbitrary `transient` flags, snapshot-store CAS conflicts and
unusable-connection errors do not authorize replay. Failed finalization in an
attempt or its children also forbids replay, even if it looks like BUSY.

Failed rollback fences the connection and retains its aggregate causes.
Finalization failure prohibits replay even when rollback succeeds. A lost
commit acknowledgement is not treated as evidence that
nothing committed. An acknowledged COMMIT remains successful when cancellation
or the deadline arrives after its dispatch. The API provides safe bounded
re-execution after confirmed recovery, not an exactly-once external-effects
protocol or a guarantee that every contention schedule eventually succeeds.

The same SDK connection remains exclusively owned through backoff. Foreign
SQL, `close()` and another transaction reject with the existing ownership
error rather than interleave between attempts. Other connections are not
blocked by this SDK lease. No native concurrency default or engine locking
policy changes.

## Policy and failure reporting

`maxAttempts` includes the first attempt (default 4, integer 1..100).
`timeoutMs` is one cooperative budget for all attempts and backoff (default
5000, integer 1..2147483647). Exponential full-jitter backoff starts at
`initialDelayMs` (default 5) and is capped by `maxDelayMs` (default 250); both
are nonnegative integer milliseconds, with initial no greater than maximum.
Even a zero delay yields a task so cancellation cannot starve behind promise
microtasks. Policies are validated before any SQL or connection lease.

`signal` cancels the whole retry operation, including backoff and the active
managed attempt. Cancellation and timeout join the callback, SQL and rollback;
they never abandon in-flight work. This is not a hard wall-clock interrupt: a
noncooperative callback or one long core operation can delay settlement beyond
the budget. Use `tx.signal` for cancellable callback work.

`FrankenTransactionRetryError` distinguishes invalid policy, cancellation and
timeout with its `code` and reports the number of started `attempts`. The
caller cancellation reason remains `cause`; `lastError` retains the latest
recovered conflict when available. Exhausting attempts rethrows the last
conflict itself, preserving SQLite codes, causes and batch indexes. Fatal
cleanup errors take precedence over cancellation/timeout wrappers.

## Executable verification

```sh
node --test packages/sdk/tests/transaction-retry.test.mjs
```

Requires Node 22.16+ with `node:sqlite` and the workspace TypeScript dependency;
`FSQLITE_TYPESCRIPT_MODULE` can point to another installed TypeScript module.
The tests execute the production SDK transaction, prepared-handle, error and
retry implementations. An explicitly injected Node-SQLite transport produces
real WAL snapshot conflicts and rollback-journal COMMIT contention, alongside
fault-injected lifecycle checks. This is not browser/worker-IPC/WASM or native
FrankenSQLite MVCC certification, and transpilation is not a workspace typecheck.
