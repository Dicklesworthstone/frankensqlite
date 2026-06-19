import { describe, expect, it } from "vitest";

import { FrankenDB } from "../src/database";
import type { WorkerResponse } from "@frankensqlite/worker";
import type { WorkerLike, WorkerMessageEvent } from "../src/worker-client";

class PreparedWorker implements WorkerLike {
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
    const response: WorkerResponse =
      message.kind === "init"
        ? {
            kind: "ready",
            requestId: message.requestId,
            data: { path: ":memory:", persistence: "memory" },
          }
        : message.kind === "prepare"
          ? {
              kind: "prepare-result",
              requestId: message.requestId,
              data: {
                statementId: "stmt-1",
                sql: message.sql,
                columnCount: 2,
                columnNames: ["id", "name"],
              },
            }
          : message.kind === "statement-query"
            ? {
                kind: "query-result",
                requestId: message.requestId,
                data: {
                  columns: ["id", "name"],
                  columnCount: 2,
                  columnTypes: ["integer", "text"],
                  rows: [{ id: 2, name: "Grace" }],
                  rowArrays: [[2, "Grace"]],
                  changes: 0,
                },
              }
            : message.kind === "statement-finalize"
              ? {
                  kind: "statement-finalize-result",
                  requestId: message.requestId,
                }
              : message.kind === "close"
                ? {
                    kind: "close-result",
                    requestId: message.requestId,
                  }
                : {
                    kind: "execute-result",
                    requestId: message.requestId,
                    changes: 1,
                  };

    queueMicrotask(() => {
      for (const listener of this.#listeners) {
        listener({ data: response });
      }
    });
  }
}

describe("FrankenPreparedStatement", () => {
  it("keeps metadata and delegates query/finalize through the worker client", async () => {
    const db = await FrankenDB.open({
      worker: new PreparedWorker(),
    });
    const stmt = await db.prepare<{ id: number; name: string }>(
      "SELECT id, name FROM demo WHERE id = ?",
    );

    expect(stmt.sql).toBe("SELECT id, name FROM demo WHERE id = ?");
    expect(stmt.columnCount).toBe(2);
    expect(stmt.columnNames).toEqual(["id", "name"]);

    const result = await stmt.query([2]);
    expect(result.rows).toEqual([{ id: 2, name: "Grace" }]);

    await expect(stmt.finalize()).resolves.toBeUndefined();
    await db.close();
  });
});
