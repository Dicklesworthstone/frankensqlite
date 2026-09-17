// Compile-only public contract; never execute this file.
import { FrankenSnapshotPool } from "../src/index";
import type { SnapshotPoolOptions, SnapshotPoolIdentity } from "../src/index";
declare const image: Uint8Array;
const settings: SnapshotPoolOptions = { workers: 2, maxPendingQueries: 4, resultEncoding: "auto" };
const readers = await FrankenSnapshotPool.open(image, settings);
const result = await readers.query<{ id: number }>("SELECT id FROM t WHERE id=:id", { id: 1 });
const id: number | undefined = result.rows[0]?.id;
const identity: SnapshotPoolIdentity = readers.snapshot;
void id; void identity;
// @ts-expect-error A pool does not expose writable connections.
await readers.execute("DELETE FROM t");
// @ts-expect-error Row field type is retained.
const wrong: string = result.rows[0]!.id;
// @ts-expect-error Binary snapshot input is required.
await FrankenSnapshotPool.open("live-database");
// @ts-expect-error Each replica requires a factory, never one shared worker object.
const bad: SnapshotPoolOptions = { worker: {} };
void wrong; void bad;
await readers.close();
