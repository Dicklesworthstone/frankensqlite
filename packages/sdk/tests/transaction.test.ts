import { describe, expect, it } from "vitest";

import { FrankenDB } from "../src/database";
import type { WorkerResponse } from "@frankensqlite/worker";
import type { WorkerLike, WorkerMessageEvent } from "../src/worker-client";

class TransactionWorker implements WorkerLike {
  readonly requests: Array<{ kind: string; sql?: string }> = [];
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
    this.requests.push({ kind: message.kind, sql: message.sql });
    const response: WorkerResponse =
      message.kind === "init"
        ? {
            kind: "ready",
            requestId: message.requestId,
            data: { path: ":memory:", persistence: "memory" },
          }
        : message.kind === "execute-batch"
          ? {
              kind: "execute-batch-result",
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

describe("FrankenDB.transaction", () => {
  it("wraps successful work in BEGIN and COMMIT", async () => {
    const worker = new TransactionWorker();
    const db = await FrankenDB.open({ worker });

    await db.transaction(async (tx) => {
      await tx.execute("INSERT INTO demo(name) VALUES (?)", ["Ada"]);
    });

    expect(worker.requests.map((request) => request.sql ?? request.kind)).toEqual([
      "init",
      "BEGIN",
      "INSERT INTO demo(name) VALUES (?)",
      "COMMIT",
    ]);

    await db.close();
  });

  it("rolls back when the callback throws", async () => {
    const worker = new TransactionWorker();
    const db = await FrankenDB.open({ worker });

    await expect(
      db.transaction(async (tx) => {
        await tx.execute("INSERT INTO demo(name) VALUES (?)", ["Grace"]);
        throw new Error("boom");
      }),
    ).rejects.toThrow("boom");

    expect(worker.requests.map((request) => request.sql ?? request.kind)).toEqual([
      "init",
      "BEGIN",
      "INSERT INTO demo(name) VALUES (?)",
      "ROLLBACK",
    ]);

    await db.close();
  });
});
