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

## Deadlines for ordinary and nested transactions

The single-attempt `db.transaction()` and nested `tx.transaction()` APIs also
accept `{ timeoutMs }` through `TransactionOptions`. This is opt-in: ordinary
transactions have no default deadline. Values must be integer milliseconds in
1..2147483647 and are validated before BEGIN or a queue reservation.

```ts
await db.transaction(async tx => {
  await tx.execute("UPDATE counters SET value = value + 1 WHERE id = ?", [1]);
  try {
    await tx.transaction(async child => {
      // A shorter child budget is recoverable after its SAVEPOINT rollback.
      await child.execute("INSERT INTO audit(message) VALUES (?)", ["updated"]);
    }, { timeoutMs: 100 });
  } catch (error) {
    if (!(error instanceof FrankenSQLiteError) ||
        error.code !== "ERR_FSQLITE_TRANSACTION_TIMEOUT") throw error;
  }
}, { timeoutMs: 2000 });
```

Each child inherits its parent's remaining budget, including a containing
retry operation's deadline. A longer child timeout cannot extend that budget;
starting another child or retry attempt cannot reset it. A child-only expiry
can be caught after rollback while a still-live parent continues. Catching a
parent expiry never restores permission to execute or commit that parent.

Timers abort `tx.signal` to wake cooperative work. Synchronous checks of the
monotonic deadline additionally run before new transaction-scoped SQL and
before commit, so delayed timer tasks or resolved-Promise chains cannot permit
a late commit. Expiry during BEGIN waits for its acknowledgement, then rolls
back without starting the callback. Admitted SQL, callbacks, child scopes and
prepared-handle cleanup are joined before rollback and rejection. Nothing
returns early via a timeout/SQL promise race. Timers are released on every
finished scope. This does not interrupt a long-running core operation or an
uncooperative callback and does not guarantee settlement within the budget.

An ordinary or child deadline reports `ERR_FSQLITE_TRANSACTION_TIMEOUT`;
caller cancellation retains `ERR_FSQLITE_TRANSACTION_CANCELLED` and its exact
local reason. A containing retry deadline reports
`ERR_FSQLITE_TRANSACTION_RETRY_TIMEOUT` after confirmed recovery, including
when both parent and child report cancellation. Mixed application failures
and failed cleanup retain their original error tree instead of being hidden
under a timeout. Successful COMMIT dispatched before expiry remains successful
even when its acknowledgement arrives after the deadline.

`queue.transaction(work, { timeoutMs, waitTimeoutMs })` separates time waiting
to start from the active transaction budget, just like queued retries. Policy
is captured at submission; the active budget starts after queue-boundary
subscription reconciliation. Journal verification and dirty-bit collection
are inside that budget. Expiry rolls back watched writes without advancing
`changeSequence`, and the FIFO slot stays occupied until recovery finishes.

## FIFO jobs and subscriptions

`FrankenDBQueue.transactionWithRetry(work, options)` exposes the same opt-in
policy without giving callers the queue's private connection. It reserves one
job slot for the whole operation, including every retry, rollback and backoff.
Accepted/completed/failed job counters count jobs, not attempts. Later jobs and
export/checkpoint barriers cannot overtake a retrying job. `close()` drains
accepted work rather than cutting an attempt short.

`QueuedTransactionRetryOptions` adds `waitTimeoutMs`, the existing queue-start
budget. It is separate from `timeoutMs`, which begins when the retry operation
starts after queue-boundary subscription reconciliation. Neither timer abandons
active SQL. Cancelling while waiting prevents all attempt callbacks; cancelling
after start drains the active attempt. Options are captured and validated at
submission, so mutating them while a job waits cannot change its retry policy.

For watched tables, schema verification, callback execution, dirty-bit
collection and clearing all belong to the same retry attempt. Failed attempts
roll back their dirty bits. Only the final acknowledged COMMIT advances
`changeSequence` and publishes its actual dirty table set; rolled-back writes
and postlude failures produce no phantom notifications. Listener failure after
commit cannot replay SQL. These remain local queue invalidations, not a durable
changefeed or notifications of another connection's writes.

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
