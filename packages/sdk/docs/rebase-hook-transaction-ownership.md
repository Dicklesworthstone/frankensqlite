# Rebase hook SQL belongs to the application transaction

`applyChangeset({ onRebase })` invokes its hook after row application and before
recording the delivery receipt. The hook's SQL executor is now scoped: admitted
`execute` and `query` calls must settle before receipt insertion or rollback,
including calls that the hook neglected to await. A retained executor rejects
new SQL after the hook exits, before entering the database adapter.

An admitted SQL failure aborts the whole application even when the hook catches
it. Only the first SQL failure is retained. An exception thrown by the hook has
precedence over child-SQL failures, but the children still drain before rollback.
Cancellation and deadlines do not abandon in-flight SQL. Constraint errors, hook
errors, and commit failure undo application rows, saved rebase decisions, and the
inbox receipt together. Replaying an existing receipt continues to skip the hook;
a lost commit response is recovered using the same delivery ID and payload.

The hook must still use the supplied executor, avoid transaction-control SQL,
and save its decisions in this database rather than publishing external effects.
The wrapper is transaction ownership, not a SQL sandbox, background task manager,
SQL queue, automatic retry, or global writer lock. Work submitted through an
unrelated handle or after the hook's lifetime is not enrolled in this scope.

## Executed regression coverage

With Node 22.16.0 and SQLite 3.49.1:

```sh
node --experimental-transform-types \
  --experimental-loader=./packages/sdk/tests/helpers/production-source-loader.mjs \
  --test packages/sdk/tests/changeset-rebase-hook-lifetime.test.mjs
```

The 20 tests run production application and codec modules on reference SQLite
transaction ownership. Fifteen fail on the original application implementation;
all 20 pass after the fix. Explicit SQL barriers exercise unawaited writes and
INSERT RETURNING, receipt ordering, callback-error precedence, caught constraints,
falsy rejection values, retained handles, cancellation, deadlines, all three
SQLite encodings, native Session conflict decisions, and lost-commit recovery.
Strict TypeScript checks cover actual source imports without declaration stubs.
This does not certify the FrankenSQLite Rust/WASM engine, full SDK/worker
packaging, browser persistence, or physical power-loss behavior.
