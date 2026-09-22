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

// Await a fresh confirmed empty observation after that source commit.
const observed = await delivery.flush({ timeoutMs: 30_000 });
console.log(observed.deliveries); // cumulative successful-run count

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

## Awaited delivery barriers

After awaiting the source/outbox commit, `await delivery.flush()` requests a
fresh drain and waits for a successful `empty` result from a pump run that
**started after the flush call**. An older in-flight empty read cannot satisfy
it: that read could have happened before the source commit. Limited pages are
followed by further runs until an eligible empty read is observed. Multiple
callers coalesce onto the same single-flight drain, not parallel pump calls.

The returned immutable statistics snapshot is cumulative, not a per-call delta.
The empty observation is not permanent emptiness, a persistent delivery cursor,
or protection from a producer appending later. It retains the existing pump's
source-confirmation and receiver-acknowledgement meaning; it adds no stronger
storage guarantee. Never use a wakeup alone as proof of delivery.

`flush({ signal, timeoutMs })` bounds only that caller's wait. Cancellation or
timeout removes the waiter and its timer/listener without cancelling shared
transport, SQL, confirmation, or another waiter. The monotonic deadline is
checked before accepting an empty result even when timer delivery is starved.
The worker continues delivery after a waiter stops waiting. Use and await
`stop({ abort: true })` to stop the worker itself.

By default at most 1024 flush waiters can be outstanding; set
`maxPendingFlushes` in 1..65536 to change that bound. Overload rejects with
`ERR_FSQLITE_DELIVERY_FLUSH_LIMIT`; `stats.pendingFlushes` exposes current
admission. Invalid inputs, cancellation, timeout and stopped ownership use
the corresponding `ChangesetDeliveryFlushError` code suffixes `INPUT`,
`CANCELLED`, `TIMEOUT` and `STOPPED`. A failed worker rejects outstanding
barriers with its original worker error retained as the cause. No failure
implies rollback or authorizes replay under a new delivery identity.

Graceful `stop()` still joins only its active bounded run. A barrier requiring
an unstarted follow-up run rejects after shutdown settles; it does not silently
change stop into an unbounded drain. A qualifying successful empty result from
an already-admitted run is retained. Await `flush()` **before** `stop()` when
the application needs a drain-to-empty boundary, and stop source producers
first when using that boundary as part of application shutdown.

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
node --experimental-transform-types --test packages/sdk/tests/changeset-worker*.test.mjs
```

The tests execute the production supervisor with explicit pump drivers to cover
single-flight ownership, wakeups, per-run budgets, task fairness, late commit
outcomes, shutdown joins, malformed results, fresh-empty barriers, waiter
admission/cancellation/deadlines and failure fencing. They do not
substitute a pump driver for certification of SQLite, WASM, browser persistence,
HTTP authentication, or the source/receiver confirmation barriers.
