// Compile-only public contract; never execute this file.

import type {
  SnapshotPoolIdentity,
  SnapshotPoolOptions,
  SnapshotQueryResult,
  SnapshotRefreshResult,
} from "../src/index";
import { FrankenSnapshotPool } from "../src/index";

declare const image: Uint8Array;
const settings: SnapshotPoolOptions = { workers: 2, maxPendingQueries: 4, resultEncoding: "auto" };
const readers = await FrankenSnapshotPool.open(image, settings);
const result = await readers.query<{ id: number }>("SELECT id FROM t WHERE id=:id", { id: 1 });
const id: number | undefined = result.rows[0]?.id;
const identity: SnapshotPoolIdentity = readers.snapshot;
const typed: SnapshotQueryResult<{ id: number }> = result;
const refreshed: SnapshotRefreshResult = await readers.refresh(image);
const generation: number = typed.snapshot.generation;
const failures: readonly unknown[] = refreshed.cleanupErrors;
void id;
void identity;
void generation;
void failures;
// @ts-expect-error Snapshot identity is read-only.
result.snapshot.generation = 99;
// @ts-expect-error Published cleanup records are read-only.
refreshed.cleanupErrors.push(new Error("not writable"));
// @ts-expect-error A pool does not expose writable connections.
await readers.execute("DELETE FROM t");
// @ts-expect-error Row field type is retained.
const wrong: string = result.rows[0]!.id;
// @ts-expect-error Binary snapshot input is required.
await FrankenSnapshotPool.open("live-database");
// @ts-expect-error Each replica requires a factory, never one shared worker object.
const bad: SnapshotPoolOptions = { worker: {} };
void wrong;
void bad;
await readers.close();
