// Compile-only public entrypoint contracts, including intentional misuse.
import { scanTable, FrankenDBQueue } from "../src/index";
import type { TableScan, TableScanOptions, TableScanStats } from "../src/index";
declare const queue: FrankenDBQueue;
declare const signal: AbortSignal;
const options: TableScanOptions = { columns: ["id", "value"], batchSize: 128, reverse: true,
  idleTimeoutMs: 5000, waitTimeoutMs: 1000, signal };
const scan: TableScan<{ id: bigint; value: string }> = scanTable(queue, "items", options);
const done: Promise<void> = scan.done;
const stats: Readonly<TableScanStats> = scan.stats;
const closed: boolean = scan.closed;
const returned: Promise<IteratorResult<{ id: bigint; value: string }>> = scan.return();
for await (const row of scan) {
  const id: bigint = row.id;
  const value: string = row.value;
  // @ts-expect-error Typed projection has no undeclared columns.
  const missing = row.unknown;
  void [id, value, missing];
}
// @ts-expect-error Input is an identifier, not an arbitrary value.
scanTable(queue, 1);
// @ts-expect-error Projection columns must be names, not SQL parameter values.
scanTable(queue, "items", { columns: [1] });
// @ts-expect-error Readonly result statistics cannot be mutated.
scan.stats.rowsRead = 2;
// @ts-expect-error Scan does not accept arbitrary SQL predicates.
scanTable(queue, "items", { where: "1=1" });
void [done, stats, closed, returned];
