import { describe, expect, it } from "vitest";

import { FrankenDB } from "../src/database";
import type { WorkerResponse } from "@frankensqlite/worker";
import type { WorkerLike, WorkerMessageEvent } from "../src/worker-client";

class MockWorker implements WorkerLike {
  readonly requests: unknown[] = [];
  readonly #listeners = new Set<(event: WorkerMessageEvent) => void>();

  addEventListener(
    type: "message" | "error",
    listener: ((event: WorkerMessageEvent) => void) | ((event: { message: string }) => void),
  ): void {
    if (type === "message") {
      this.#listeners.add(listener as (event: WorkerMessageEvent) => void);
    }
  }

  removeEventListener(
    type: "message" | "error",
    listener: ((event: WorkerMessageEvent) => void) | ((event: { message: string }) => void),
  ): void {
    if (type === "message") {
      this.#listeners.delete(listener as (event: WorkerMessageEvent) => void);
    }
  }

  postMessage(message: any): void {
    this.requests.push(message);
    queueMicrotask(() => {
      const response: WorkerResponse =
        message.kind === "init"
          ? {
              kind: "ready",
              requestId: message.requestId,
              data: { path: message.config.dbName ?? ":memory:", persistence: "memory" },
            }
          : message.kind === "query"
            ? {
                kind: "query-result",
                requestId: message.requestId,
                data: {
                  columns: ["id", "name"],
                  columnCount: 2,
                  columnTypes: ["integer", "text"],
                  rows: [{ id: 1, name: "Ada" }],
                  rowArrays: [[1, "Ada"]],
                  changes: 0,
                },
              }
            : message.kind === "export"
              ? {
                  kind: "export-result",
                  requestId: message.requestId,
                  data: Uint8Array.of(9, 8, 7),
                }
              : {
                  kind:
                    message.kind === "close"
                      ? "close-result"
                      : message.kind === "execute-batch"
                        ? "execute-batch-result"
                        : "execute-result",
                  requestId: message.requestId,
                  changes: 1,
                };
      for (const listener of this.#listeners) {
        listener({ data: response });
      }
    });
  }
}

describe("FrankenDB", () => {
  it("opens through a worker and exposes async query/export methods", async () => {
    const worker = new MockWorker();
    const db = await FrankenDB.open({
      dbName: "browser-demo",
      worker,
    });

    expect(db.path).toBe("browser-demo");
    expect(await db.execute("CREATE TABLE demo(id INTEGER PRIMARY KEY)")).toBe(1);

    const result = await db.query<{ id: number; name: string }>(
      "SELECT id, name FROM demo",
    );
    expect(result.rows).toEqual([{ id: 1, name: "Ada" }]);

    const snapshot = await db.export();
    expect([...snapshot]).toEqual([9, 8, 7]);

    await db.close();
  });
});
