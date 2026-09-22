# Supervised continuous changeset delivery

`ChangesetDeliveryWorker` owns repeated, bounded runs of one existing
`ChangesetDeliveryPump`. It continues after a per-run delivery/byte limit, polls
after an empty read, and accepts coalesced wakeups after new source commits.
There is only one active pump call and no prefetched payload queue.

```ts
import { ChangesetDeliveryPump, ChangesetDeliveryWorker } from "@frankensqlite/sdk";

// Use the existing authenticated transport and source-confirmation barrier.
const pump = new ChangesetDeliveryPump(outbox, {
  receiverId: "replica-1",
  deliver,
  confirmSource,
});
const delivery = ChangesetDeliveryWorker.start(pump, {
  maxDeliveriesPerRun: 100,
  maxBytesPerRun: 8 * 1024 * 1024,
  pollIntervalMs: 1000,
});

// After awaiting a new outbox.record(...) commit:
delivery.notify(); // a wakeup, not a delivery acknowledgement

// Finish the current bounded run, then release this runner's ownership.
await delivery.stop();
// Only now close the source connection or replace the runner.
```

The caller must own the pump exclusively until `done` settles. Do not also call
`pump.run()` independently. `stopWhenIdle: true` provides a bounded-per-turn
drain-to-empty task instead of a persistent polling session. Empty means no
pending row at the pump's last read, not that another producer cannot append.
`start()` never invokes the pump before returning its owner.

Every completed run yields a task turn before another run, even when all pump
promises resolve immediately and notifications arrive continuously. Notifications
are a single coalescing flag, not an unbounded queue. They never skip the pump's
oldest pending entry, source confirmation or exact receiver acknowledgement.

## Shutdown and failure

`stop()` finishes only the active bounded run; it does not promise to drain the
whole outbox. Repeated calls return the same `done` promise. An idle stop cancels
the poll timer without starting another run. `stop({ abort: true, reason })`, or
the optional external `signal`, additionally signals cooperative cancellation
to the active pump. Graceful shutdown may be escalated to abort.

No timer races an in-flight SQL operation, transport or confirmation against a
fake successful shutdown. The actual pump outcome must settle first. A commit
that completes despite late cancellation remains a success. If the pump rejects,
`done` and `stop()` reject with `ChangesetDeliveryWorkerError`; its `cause` retains
the original delivery phase, identity and uncertainty. Aborting does not hide
an unrelated storage failure. An individual operation that ignores cancellation
can still delay shutdown indefinitely.

There are **no automatic retries after failure**. In particular, a lost remote
acknowledgement, source checkpoint failure, receipt mismatch, SQL conflict, or
malformed pump result stops admission. Reconcile the existing outbox/receiver
and checkpoint state before creating another runner. Do not manufacture a new
delivery identity or assume the failed run rolled back. Retained receiver receipts
and the original pump's replay rules remain the source of idempotence.

This is an in-process supervisor, not a new Web Worker, service worker, native
MVCC coordinator, page-level VFS, or browser background-execution guarantee. It
does not change persistence policy, close the database, acquire a global writer
lock, or acknowledge an unconfirmed snapshot.

## Limits and statistics

Defaults are 100 selections and 64 MiB of payload per pump run, with a 1000 ms
idle poll. Bounds are 1..10,000 selections, 1..1 GiB per run and
1..2,147,483,647 ms for polling or the optional `runTimeoutMs` budget. The pump's
own per-message cap still applies. A run timeout is cooperative and is a failure,
not permission to abandon a pending commit and start another run.

`stats` is an immutable snapshot containing state, active-run status, attempts,
successful runs, and the pump's delivery/byte/decision/replay/acknowledgement-race
totals. Totals include only successful runs. A failed run may already have
confirmed earlier deliveries, so these counters can undercount committed work;
they are not a durable outbox cursor or an exactly-once claim.

## Verification

```sh
node --experimental-transform-types --test packages/sdk/tests/changeset-worker.test.mjs
```

The tests execute the production supervisor with explicit pump drivers to cover
single-flight ownership, wakeups, per-run budgets, task fairness, late commit
outcomes, shutdown joins, malformed results and failure fencing. They do not
substitute a pump driver for certification of SQLite, WASM, browser persistence,
HTTP authentication, or the source/receiver confirmation barriers.
