// Compile-only public entrypoint contract; never execute intentional misuse.
import { watchQuery, FrankenDBQueue, FrankenSnapshotPool } from "../src/index";
import type { LiveQuery, LiveQueryResult } from "../src/index";
async function contract(queue: FrankenDBQueue): Promise<void> {
  const query: LiveQuery<{ id: number; value: string }> = await watchQuery(queue,
    "SELECT id,value FROM items WHERE id >= :id",
    { tables: ["items"], params: { id: 1 }, maxInputBytes: 1024,
      signal: new AbortController().signal, waitTimeoutMs: 500 });
  for await (const result of query) {
    const typed: LiveQueryResult<{ id: number; value: string }> = result;
    const value: string | undefined = typed.rows[0]?.value;
    const sequence: bigint = result.throughSequence;
    void value; void sequence;
    // @ts-expect-error Local sequences are bigint, not lossy numbers.
    const numberSequence: number = result.throughSequence;
    // @ts-expect-error Sequence metadata is read-only.
    result.throughSequence = 2n;
    void numberSequence;
    break;
  }
  await query.done;
  // @ts-expect-error Dependency tables are required.
  await watchQuery(queue, "SELECT * FROM items", {});
  // @ts-expect-error Use an array of dependency names, not a SQL fragment.
  await watchQuery(queue, "SELECT * FROM items", { tables: "items" });
  // @ts-expect-error Parameter objects only contain SQL scalar values.
  await watchQuery(queue, "SELECT :id", { tables: ["items"], params: { id: {} } });
  // @ts-expect-error Iterator state is immutable.
  query.closed = false;
  void FrankenSnapshotPool; // The existing public pool export remains present.
}
void contract;
