# Capture owns admitted callback SQL through settlement

Both `captureChangeset` (the first-touch journal) and `captureSnapshotChangeset`
(the opt-in before/after scan) give their callback a scoped executor. The same
boundary applies to `ChangesetOutbox.record` and `recordSnapshot`: capture runs
inside the source publication transaction, not before a separate enqueue.

Every SQL call submitted through that executor before the callback exits must
settle before row collection, observation-trigger cleanup, outbox publication,
or transaction rollback. This includes an unawaited `execute` and a mutating
`query` such as `INSERT ... RETURNING`. Normal application code should still
await its calls. Deferring execution in an asynchronous SQL adapter no longer
allows capture to return an empty changeset and then commit an unrecorded write.

The executor closes when the callback settles, on success or failure. A saved
executor rejects all subsequent `execute` and `query` calls without entering the
SQL adapter, including during a later transaction. It does not retain a growing
list of errors from rejected late use. The callback's returned value is preserved.

An error from ANY admitted SQL call rejects capture, even when application code
catches that error. The source owner must roll back the complete scope rather
than publish successful earlier statements as a complete business operation.
Only the first SQL failure is retained. A thrown callback error remains the
primary failure, but admitted SQL is drained before that error reaches the owner.
This brings journal capture into line with snapshot capture and the SDK's failed-
transaction ownership model. Use explicit conflict-handling SQL for expected
business alternatives, rather than catching failed statements inside capture.

Cancellation and timeout do not race cleanup or rollback against already-started
SQL. The existing cooperative controls are checked at submission, completion and
the final drain boundary. A target that never settles an admitted operation can
therefore delay completion. No retry, background task, new waiting queue or global
writer lock is introduced. The actual SQL owner retains its execution ordering,
admission/memory limits and transaction/conflict behavior.

This is ownership composition, not a sandbox. Callbacks must not issue raw
transaction-control SQL, change capture schemas/connection policy, or use another
reference to the same connection for unrelated work. Promises or timers that have
not submitted SQL before callback closure are not part of capture; later attempts
through the closed executor reject. Nested captures remain provisional until the
outer owner commits. Returned bytes alone are not storage confirmation, and a
lost source COMMIT response still requires retry with the same retained outbox ID.

## Verification

Run with Node 22.16.0 or a compatible runtime:

```sh
node --experimental-transform-types \
  --experimental-loader=./packages/sdk/tests/helpers/production-source-loader.mjs \
  --test packages/sdk/tests/changeset-capture-lifetime.test.mjs
```

The 40 tests run actual journal capture, snapshot capture, outbox, store, fanout,
codec and `applyChangeset` modules over a reference SQLite transaction owner.
They hold actual SQL calls behind explicit barriers to prove that no collection,
COMMIT or ROLLBACK overtakes them. The three initial journal regressions fail
against unchanged upstream. Other cases cover late reads/writes, subsequent
transactions, callback errors, caught constraints, cancellation, deadlines,
mutating queries, lost COMMIT responses, native Session comparison in UTF-8 and
both UTF-16 encodings, and retention for two required replicas. No replication
backend or type dependency is substituted; the loader only resolves imports.

These are SDK-component tests over Node's SQLite, not FrankenSQLite Rust/WASM/MVCC
execution or full SDK/worker packaging. Browser checkpoints, deployment, physical
power loss and non-cooperating adapters are not certified. Native writer defaults,
wire formats, dependency lists, workflows and beads are unchanged.
