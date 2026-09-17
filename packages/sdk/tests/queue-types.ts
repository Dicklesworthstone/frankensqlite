// Compile-only public API contract. Includes deliberate type errors; never run.
import { FrankenDBQueue } from "../src/index";
import type { FrankenTransaction, JobQueueStats } from "../src/index";
function accepts<T>(_value: T): void {}
export async function queueTypes(): Promise<void> {
  const queue = await FrankenDBQueue.open({ persistence: "memory" }, { maxPendingJobs: 8 });
  accepts<Promise<number>>(queue.transaction(async tx => {
    accepts<FrankenTransaction>(tx);
    const statement = await tx.prepare<{ id: number }>("SELECT :id AS id");
    return (await statement.get({ id: 42 }))!.id;
  }, { signal: new AbortController().signal, waitTimeoutMs: 1000 }));
  accepts<Promise<string>>(queue.transaction(() => "value"));
  accepts<JobQueueStats>(queue.stats);
  accepts<Promise<Uint8Array>>(queue.export({ waitTimeoutMs: 1000 }));
  accepts<Promise<import("../src/types").SnapshotMetadata>>(queue.checkpoint({ signal: new AbortController().signal }));
  accepts<Promise<void>>(queue.close());
  // @ts-expect-error callbacks receive a scoped transaction, not the raw queue/db
  queue.transaction(tx => tx.close());
  // @ts-expect-error every submitted job must be a callback
  queue.transaction("INSERT INTO items VALUES(1)");
  // @ts-expect-error async result inference is preserved
  accepts<Promise<number>>(queue.transaction(() => "string"));
  // @ts-expect-error stats are read-only snapshots
  queue.stats.pendingJobs = 0;
  // @ts-expect-error the underlying connection is not exposed
  queue.db.execute("COMMIT");
  // @ts-expect-error limit configuration is numeric
  FrankenDBQueue.open(undefined, { maxPendingJobs: "8" });
}
