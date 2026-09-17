// Compile-only API contract, never execute these deliberate misuse examples.
import { FrankenDBQueue } from "../src/index";
import type { CommittedTableChange, TableChangeStream, TableSubscription } from "../src/index";
async function contract(queue: FrankenDBQueue): Promise<void> {
  const sub: TableSubscription = await queue.subscribe(["items"], async (change: CommittedTableChange) => {
    const sequence: bigint = change.lastSequence;
    const commits: bigint = change.commits;
    const tables: readonly string[] = change.tables;
    await queue.transaction(tx => tx.query("SELECT * FROM items"));
    void sequence; void commits; void tables;
    // @ts-expect-error Change tables are immutable.
    change.tables.push("other");
    // @ts-expect-error Sequences are BigInt, not lossy numbers.
    const numberSequence: number = change.firstSequence;
    void numberSequence;
  }, { signal: new AbortController().signal, waitTimeoutMs: 1000 });
  sub.unsubscribe(); await sub.done;
  const changes: TableChangeStream = await queue.changes(["items"]);
  for await (const change of changes) {
    const sequence: bigint = change.lastSequence;
    void sequence;
    break;
  }
  await changes.return?.();
  // @ts-expect-error Stream lifecycle state is immutable.
  changes.closed = false;
  // @ts-expect-error Tables must be identifiers, not a single SQL string.
  await queue.subscribe("items", () => {});
  // @ts-expect-error Listeners may not return an arbitrary numeric payload.
  await queue.subscribe(["items"], () => 123);
  // @ts-expect-error Status is read-only.
  sub.state = "active";
}
void contract;
