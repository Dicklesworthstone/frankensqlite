import { describe, expect, it } from "vitest";
import type { WorkerRequest, WorkerResponse } from "@frankensqlite/worker";

import { FrankenDB } from "../src/database";
import { FrankenSQLiteError } from "../src/errors";
import type { FrankenTransaction } from "../src/transaction";
import { ControlledWorker, deferred, drain, observe, rejected } from "./helpers/controlled-worker";

async function fixture() {
  const worker = new ControlledWorker();
  const held = new Set<string>();
  const failures = new Map<string, string>();
  const waiting: WorkerRequest[] = [];
  const names = new Map<string, string>();
  const failedAt = new Map<number, number>();
  // This controlled transport traces logical boundary stages for these SDK
  // lifetime tests. Real worker SQL/fences are covered in managed-transactions.
  // One typed child rollback includes both rollback-to and cleanup release.
  const stages = (request: WorkerRequest): string[] => {
    if (request.kind !== "transaction") return ["sql" in request ? request.sql : request.kind];
    if (request.action === "begin" && request.parentId !== undefined && !names.has(request.transactionId)) {
      names.set(request.transactionId, `fsqlite_sdk_${names.size + 1}`);
    }
    const name = names.get(request.transactionId);
    if (name === undefined) return [request.action.toUpperCase()];
    if (request.action === "begin") return [`SAVEPOINT ${name}`];
    if (request.action === "commit") return [`RELEASE SAVEPOINT ${name}`];
    return [`ROLLBACK TO SAVEPOINT ${name}`, `RELEASE SAVEPOINT ${name}`];
  };
  const key = (request: WorkerRequest) => stages(request)[0]!;
  function reply(request: WorkerRequest): void {
    const keys = stages(request);
    for (let i = 0; i < keys.length; i++) {
      const failure = failures.get(keys[i]!);
      if (failure !== undefined) {
        failedAt.set(request.requestId, i + 1);
        worker.reply({ kind: "error", requestId: request.requestId,
          error: { code: "SQLITE_ERROR", message: failure } });
        return;
      }
    }
    const requestId = request.requestId;
    let response: WorkerResponse;
    switch (request.kind) {
      case "init": response = { kind: "ready", requestId, data: { path: ":memory:", persistence: "memory" } }; break;
      case "execute-batch": response = { kind: "execute-batch-result", requestId }; break;
      case "transaction": response = { kind: "transaction-result", requestId }; break;
      case "execute": case "statement-execute": response = { kind: "execute-result", requestId, changes: 1 }; break;
      case "prepare": response = { kind: "prepare-result", requestId, data: {
        statementId: String(requestId), sql: request.sql, columnCount: 1, columnNames: ["value"],
      } }; break;
      case "query": case "statement-query": response = { kind: "query-result", requestId, data: {
        columns: ["value"], columnCount: 1, columnTypes: ["integer"], rows: [{ value: 1 }], rowArrays: [[1]], changes: 0,
      } }; break;
      case "statement-finalize": response = { kind: "statement-finalize-result", requestId }; break;
      case "export": response = { kind: "export-result", requestId, data: Uint8Array.of(1) }; break;
      case "close": response = { kind: "close-result", requestId }; break;
      default: throw new Error(`Unexpected fixture request ${request.kind}`);
    }
    worker.reply(response);
  }
  worker.onPost = (request) => {
    if (held.has(key(request))) waiting.push(request);
    else queueMicrotask(() => reply(request));
  };
  const db = await FrankenDB.open({ worker });
  worker.requests.length = 0;
  return {
    db, worker, held, failures,
    log: () => worker.requests.flatMap(request => stages(request).slice(0, failedAt.get(request.requestId))),
    release(sql: string) {
      held.delete(sql);
      for (const request of waiting.splice(0)) {
        if (key(request) === sql) reply(request);
        else waiting.push(request);
      }
    },
  };
}

function expectOwnershipError(error: unknown): void {
  expect(error instanceof FrankenSQLiteError).toBe(true);
  expect((error as FrankenSQLiteError).code).toBe("ERR_FSQLITE_TRANSACTION_OWNERSHIP");
}

describe("managed transaction ownership", () => {
  it("reserves ownership before awaiting BEGIN", async () => {
    const f = await fixture();
    f.held.add("BEGIN");
    const transaction = observe(f.db.transaction(async () => 7));
    const foreign = observe(f.db.execute("FOREIGN"));
    await drain();
    const beforeRelease = f.log();
    const foreignOutcome = foreign.outcome;
    f.release("BEGIN");
    await transaction.settled;
    expectOwnershipError(rejected({ outcome: foreignOutcome }));
    expect(beforeRelease).toEqual(["BEGIN"]);
    expect(transaction.outcome).toEqual({ status: "fulfilled", value: 7 });
    await f.db.close();
  });

  it("refuses every foreign database operation while a callback is suspended", async () => {
    const f = await fixture();
    const gate = deferred<void>();
    const started = deferred<void>();
    const transaction = observe(f.db.transaction(async (tx) => {
      await tx.execute("OWNED");
      started.resolve();
      await gate.promise;
    }));
    await started.promise;
    const operations: Promise<unknown>[] = [
      f.db.execute("FOREIGN"), f.db.query("FOREIGN"), f.db.executeBatch("FOREIGN"),
      f.db.prepare("FOREIGN"), f.db.export(), f.db.transaction(async () => 0), f.db.close(),
    ];
    const observations = operations.map(observe);
    await drain();
    const outcomes = observations.map((o) => o.outcome);
    gate.resolve();
    await transaction.settled;
    for (const outcome of outcomes) expectOwnershipError(rejected({ outcome }));
    expect(transaction.outcome.status).toBe("fulfilled");
    expect(f.log()).toEqual(["BEGIN", "OWNED", "COMMIT"]);
    await f.db.close();
  });

  it("keeps ownership until COMMIT settles, not just until the callback returns", async () => {
    const f = await fixture();
    f.held.add("COMMIT");
    const transaction = observe(f.db.transaction(async () => "done"));
    await drain();
    const foreign = observe(f.db.query("FOREIGN"));
    await drain();
    const outcome = foreign.outcome;
    f.release("COMMIT");
    await transaction.settled;
    expectOwnershipError(rejected({ outcome }));
    expect(f.log()).toEqual(["BEGIN", "COMMIT"]);
    await f.db.close();
  });

  it("blocks prepared handles created outside the transaction without invalidating them", async () => {
    const f = await fixture();
    const statement = await f.db.prepare("SELECT 1");
    await f.db.transaction(async () => {
      const operations = [observe(statement.execute()), observe(statement.query()), observe(statement.finalize())];
      await drain();
      for (const operation of operations) expectOwnershipError(rejected(operation));
    });
    expect(await statement.execute()).toBe(1);
    await statement.finalize();
    expect(f.log().filter((s) => s === "statement-finalize")).toHaveLength(1);
    await f.db.close();
  });

  it("invalidates escaped transaction handles after commit and rollback", async () => {
    for (const rollback of [false, true]) {
      const f = await fixture();
      let escaped!: FrankenTransaction;
      const transaction = observe(f.db.transaction(async (tx) => {
        escaped = tx;
        if (rollback) throw new Error("stop");
      }));
      await transaction.settled;
      const before = f.log();
      const operations = [observe(escaped.execute("LATE")), observe(escaped.query("LATE")), observe(escaped.prepare("LATE"))];
      await drain();
      for (const operation of operations) {
        expect((rejected(operation) as FrankenSQLiteError).code).toBe("ERR_FSQLITE_TRANSACTION_CLOSED");
      }
      expect(f.log()).toEqual(before);
      await f.db.close();
    }
  });

  it("releases transaction-owned prepared statements and rejects their escaped handles", async () => {
    const f = await fixture();
    const escaped = await f.db.transaction(async (tx) => {
      const statement = await tx.prepare("SELECT 1");
      expect((await statement.query()).rows).toEqual([{ value: 1 }]);
      return statement;
    });
    const before = f.log();
    const operations = [observe(escaped.execute()), observe(escaped.query())];
    await drain();
    for (const operation of operations) {
      expect((rejected(operation) as FrankenSQLiteError).code).toBe("ERR_FSQLITE_TRANSACTION_CLOSED");
    }
    expect(before).toEqual(["BEGIN", "SELECT 1", "statement-query", "statement-finalize", "COMMIT"]);
    expect(f.log()).toEqual(before);
    await f.db.close();
  });

  it("coalesces accepted prepared finalization and rejects reuse", async () => {
    const f = await fixture();
    await f.db.transaction(async (tx) => {
      const statement = await tx.prepare("SELECT 1");
      await Promise.all([statement.finalize(), statement.finalize()]);
      const reuse = observe(statement.execute());
      await drain();
      expect(rejected(reuse) instanceof Error).toBe(true);
    });
    expect(f.log().filter((s) => s === "statement-finalize")).toHaveLength(1);
    await f.db.close();
  });

  it("waits for unawaited admitted work before committing", async () => {
    const f = await fixture();
    f.held.add("PENDING");
    const transaction = observe(f.db.transaction(async (tx) => {
      void tx.execute("PENDING");
      return 9;
    }));
    await drain();
    const before = f.log();
    f.release("PENDING");
    await transaction.settled;
    expect(before).toEqual(["BEGIN", "PENDING"]);
    expect(f.log()).toEqual(["BEGIN", "PENDING", "COMMIT"]);
    expect(transaction.outcome).toEqual({ status: "fulfilled", value: 9 });
    await f.db.close();
  });

  it("rolls back instead of silently committing when unawaited work fails", async () => {
    const f = await fixture();
    f.held.add("PENDING");
    f.failures.set("PENDING", "write failed");
    const transaction = observe(f.db.transaction(async (tx) => {
      void tx.execute("PENDING");
    }));
    await drain();
    f.release("PENDING");
    await transaction.settled;
    expect((rejected(transaction) as Error).message).toContain("write failed");
    expect(f.log()).toEqual(["BEGIN", "PENDING", "ROLLBACK"]);
    await f.db.close();
  });

  it("does not rollback a transaction it failed to begin", async () => {
    const f = await fixture();
    f.failures.set("BEGIN", "already in transaction");
    let called = false;
    const transaction = observe(f.db.transaction(async () => { called = true; }));
    await transaction.settled;
    expect((rejected(transaction) as Error).message).toContain("already in transaction");
    expect(called).toBe(false);
    expect(f.log()).toEqual(["BEGIN"]);
    f.failures.clear();
    expect(await f.db.execute("AFTER")).toBe(1);
    await f.db.close();
  });

  it("rolls back after COMMIT fails and releases ownership", async () => {
    const f = await fixture();
    f.failures.set("COMMIT", "commit failed");
    const transaction = observe(f.db.transaction(async (tx) => { await tx.execute("OWNED"); }));
    await transaction.settled;
    expect((rejected(transaction) as Error).message).toContain("commit failed");
    expect(f.log()).toEqual(["BEGIN", "OWNED", "COMMIT", "ROLLBACK"]);
    expect(await f.db.execute("AFTER")).toBe(1);
    await f.db.close();
  });

  it("preserves the callback error when rollback also fails and prevents unsafe reuse", async () => {
    const f = await fixture();
    f.failures.set("ROLLBACK", "rollback failed");
    const cause = new Error("callback failed");
    const transaction = observe(f.db.transaction(async () => { throw cause; }));
    await transaction.settled;
    const error = rejected(transaction);
    expect(error instanceof AggregateError).toBe(true);
    expect((error as AggregateError).cause).toBe(cause);
    expect((error as AggregateError).errors[0]).toBe(cause);
    expect(((error as AggregateError).errors[1] as Error).message).toContain("rollback failed");
    const before = f.log();
    const after = observe(f.db.execute("UNSAFE"));
    await drain();
    expect(rejected(after) instanceof Error).toBe(true);
    expect(f.log()).toEqual(before);
    expect(f.worker.terminateCount).toBe(1);
  });

  it("drains and finalizes a preparation still pending at callback return", async () => {
    const f = await fixture();
    f.held.add("SELECT 1");
    const transaction = observe(f.db.transaction(async (tx) => { void tx.prepare("SELECT 1"); }));
    await drain();
    const before = f.log();
    f.release("SELECT 1");
    await transaction.settled;
    expect(before).toEqual(["BEGIN", "SELECT 1"]);
    expect(f.log()).toEqual(["BEGIN", "SELECT 1", "statement-finalize", "COMMIT"]);
    await f.db.close();
  });

  it("does not serialize independent databases", async () => {
    const first = await fixture();
    const second = await fixture();
    const gate = deferred<void>();
    const started = deferred<void>();
    const transaction = first.db.transaction(async () => { started.resolve(); await gate.promise; });
    await started.promise;
    expect(await second.db.execute("INDEPENDENT")).toBe(1);
    gate.resolve();
    await transaction;
    await Promise.all([first.db.close(), second.db.close()]);
  });
});

describe("nested transaction savepoints", () => {
  it("uses named savepoints rather than nested BEGIN and preserves callback results", async () => {
    const f = await fixture();
    const result = await f.db.transaction(async (parent) => {
      await parent.execute("PARENT");
      return parent.transaction(async (child) => {
        await child.execute("CHILD");
        return 42;
      });
    });
    expect(result).toBe(42);
    expect(f.log()).toEqual([
      "BEGIN", "PARENT", "SAVEPOINT fsqlite_sdk_1", "CHILD",
      "RELEASE SAVEPOINT fsqlite_sdk_1", "COMMIT",
    ]);
    await f.db.close();
  });

  it("rolls back and releases a failed child before the parent continues", async () => {
    const f = await fixture();
    const cause = new Error("child failed");
    await f.db.transaction(async (parent) => {
      await parent.execute("SIBLING_BEFORE");
      const child = observe(parent.transaction(async (tx) => {
        await tx.execute("DISCARDED");
        throw cause;
      }));
      await child.settled;
      expect(rejected(child)).toBe(cause);
      await parent.execute("SIBLING_AFTER");
    });
    expect(f.log()).toEqual([
      "BEGIN", "SIBLING_BEFORE", "SAVEPOINT fsqlite_sdk_1", "DISCARDED",
      "ROLLBACK TO SAVEPOINT fsqlite_sdk_1", "RELEASE SAVEPOINT fsqlite_sdk_1", "SIBLING_AFTER", "COMMIT",
    ]);
    await f.db.close();
  });

  it("allows a parent to recover from a child's SQL failure", async () => {
    const f = await fixture();
    f.failures.set("BAD_CHILD_SQL", "constraint failed");
    await f.db.transaction(async (parent) => {
      const child = observe(parent.transaction(async (tx) => {
        await tx.execute("BAD_CHILD_SQL").catch(() => {});
      }));
      await child.settled;
      expect((rejected(child) as Error).message).toContain("constraint failed");
      await parent.execute("RECOVERED");
    });
    expect(f.log()).toEqual([
      "BEGIN", "SAVEPOINT fsqlite_sdk_1", "BAD_CHILD_SQL",
      "ROLLBACK TO SAVEPOINT fsqlite_sdk_1", "RELEASE SAVEPOINT fsqlite_sdk_1", "RECOVERED", "COMMIT",
    ]);
    await f.db.close();
  });

  it("rejects parent, sibling and parent-prepared operations while a child owns the connection", async () => {
    const f = await fixture();
    await f.db.transaction(async (parent) => {
      const statement = await parent.prepare("SELECT 1");
      const gate = deferred<void>();
      const started = deferred<void>();
      const child = parent.transaction(async (tx) => { started.resolve(); await gate.promise; await tx.execute("CHILD"); });
      await started.promise;
      const operations: Promise<unknown>[] = [
        parent.execute("FOREIGN"), parent.query("FOREIGN"), parent.prepare("FOREIGN"),
        parent.transaction(async () => 0), statement.execute(), statement.query(), statement.finalize(),
      ];
      const results = operations.map(observe);
      await drain();
      const outcomes = results.map((r) => r.outcome);
      gate.resolve();
      await child;
      for (const outcome of outcomes) expectOwnershipError(rejected({ outcome }));
      expect(await statement.execute()).toBe(1);
    });
    expect(f.log().includes("FOREIGN")).toBe(false);
    expect(f.log().filter((s) => s.startsWith("SAVEPOINT "))).toHaveLength(1);
    await f.db.close();
  });

  it("holds child ownership while SAVEPOINT is still pending", async () => {
    const f = await fixture();
    f.held.add("SAVEPOINT fsqlite_sdk_1");
    await f.db.transaction(async (parent) => {
      const child = parent.transaction(async () => 1);
      const foreign = observe(parent.execute("FOREIGN"));
      await drain();
      const outcome = foreign.outcome;
      f.release("SAVEPOINT fsqlite_sdk_1");
      expect(await child).toBe(1);
      expectOwnershipError(rejected({ outcome }));
    });
    expect(f.log()).toEqual(["BEGIN", "SAVEPOINT fsqlite_sdk_1", "RELEASE SAVEPOINT fsqlite_sdk_1", "COMMIT"]);
    await f.db.close();
  });

  it("keeps ownership through RELEASE and only then restores the parent", async () => {
    const f = await fixture();
    f.held.add("RELEASE SAVEPOINT fsqlite_sdk_1");
    await f.db.transaction(async (parent) => {
      const child = parent.transaction(async (tx) => { await tx.execute("CHILD"); });
      await drain();
      const foreign = observe(parent.execute("FOREIGN"));
      await drain();
      const outcome = foreign.outcome;
      f.release("RELEASE SAVEPOINT fsqlite_sdk_1");
      await child;
      expectOwnershipError(rejected({ outcome }));
      await parent.execute("AFTER");
    });
    expect(f.log()).toEqual(["BEGIN", "SAVEPOINT fsqlite_sdk_1", "CHILD", "RELEASE SAVEPOINT fsqlite_sdk_1", "AFTER", "COMMIT"]);
    await f.db.close();
  });

  it("supports multiple nesting levels in strict stack order", async () => {
    const f = await fixture();
    await f.db.transaction((parent) => parent.transaction((child) => child.transaction(async (grandchild) => {
      await grandchild.execute("DEEPEST");
    })));
    expect(f.log()).toEqual([
      "BEGIN", "SAVEPOINT fsqlite_sdk_1", "SAVEPOINT fsqlite_sdk_2", "DEEPEST",
      "RELEASE SAVEPOINT fsqlite_sdk_2", "RELEASE SAVEPOINT fsqlite_sdk_1", "COMMIT",
    ]);
    await f.db.close();
  });

  it("invalidates a child handle as soon as that child's callback finishes", async () => {
    const f = await fixture();
    await f.db.transaction(async (parent) => {
      const child = await parent.transaction((tx) => tx);
      const late = [observe(child.execute("LATE")), observe(child.transaction(async () => 0))];
      await drain();
      for (const result of late) expect((rejected(result) as FrankenSQLiteError).code).toBe("ERR_FSQLITE_TRANSACTION_CLOSED");
      await parent.execute("PARENT_STILL_VALID");
    });
    expect(f.log().includes("LATE")).toBe(false);
    await f.db.close();
  });

  it("does not rollback or release a savepoint that failed to start", async () => {
    const f = await fixture();
    f.failures.set("SAVEPOINT fsqlite_sdk_1", "savepoint failed");
    await f.db.transaction(async (parent) => {
      let called = false;
      const child = observe(parent.transaction(() => { called = true; }));
      await child.settled;
      expect((rejected(child) as Error).message).toContain("savepoint failed");
      expect(called).toBe(false);
      await parent.execute("STILL_VALID");
    });
    expect(f.log()).toEqual(["BEGIN", "SAVEPOINT fsqlite_sdk_1", "STILL_VALID", "COMMIT"]);
    await f.db.close();
  });

  it("never releases a failed rollback-to or commits the outer transaction", async () => {
    const f = await fixture();
    f.failures.set("ROLLBACK TO SAVEPOINT fsqlite_sdk_1", "rollback-to failed");
    const cause = new Error("child failure");
    const transaction = observe(f.db.transaction((parent) => parent.transaction(() => { throw cause; })));
    await transaction.settled;
    const error = rejected(transaction);
    expect(error instanceof AggregateError).toBe(true);
    expect((error as AggregateError).cause).toBe(cause);
    expect(((error as AggregateError).errors[1] as Error).message).toContain("rollback-to failed");
    expect(f.log()).toEqual(["BEGIN", "SAVEPOINT fsqlite_sdk_1", "ROLLBACK TO SAVEPOINT fsqlite_sdk_1"]);
    expect(f.worker.terminateCount).toBe(1);
  });

  it("fails closed when RELEASE fails after a successful rollback-to", async () => {
    const f = await fixture();
    f.failures.set("RELEASE SAVEPOINT fsqlite_sdk_1", "release failed");
    const cause = new Error("child failure");
    const transaction = observe(f.db.transaction(async (parent) => {
      await parent.transaction(() => { throw cause; }).catch(() => {});
    }));
    await transaction.settled;
    const error = rejected(transaction);
    expect(error instanceof AggregateError).toBe(true);
    expect((error as AggregateError).cause).toBe(cause);
    expect(f.worker.terminateCount).toBe(1);
    expect(f.log()).toEqual([
      "BEGIN", "SAVEPOINT fsqlite_sdk_1", "ROLLBACK TO SAVEPOINT fsqlite_sdk_1", "RELEASE SAVEPOINT fsqlite_sdk_1",
    ]);
  });

  it("drains a child that outlives its parent callback and rolls back instead of committing early", async () => {
    const f = await fixture();
    const gate = deferred<void>();
    const started = deferred<void>();
    const transaction = observe(f.db.transaction(async (parent) => {
      void parent.transaction(async (child) => {
        started.resolve();
        await gate.promise;
        await child.execute("CHILD");
      });
    }));
    await started.promise;
    await drain();
    const before = f.log();
    gate.resolve();
    await transaction.settled;
    expect((rejected(transaction) as FrankenSQLiteError).code).toBe("ERR_FSQLITE_TRANSACTION_UNAWAITED");
    expect(before).toEqual(["BEGIN", "SAVEPOINT fsqlite_sdk_1"]);
    expect(f.log()).toEqual([
      "BEGIN", "SAVEPOINT fsqlite_sdk_1", "CHILD", "RELEASE SAVEPOINT fsqlite_sdk_1", "ROLLBACK",
    ]);
    await f.db.close();
  });
});

describe("savepoint cleanup failure preservation", () => {
  it("can recover after a RELEASE error when rollback-to and its RELEASE succeed", async () => {
    const f = await fixture();
    f.held.add("RELEASE SAVEPOINT fsqlite_sdk_1");
    await f.db.transaction(async (parent) => {
      const child = observe(parent.transaction(async (tx) => { await tx.execute("CHILD"); }));
      await drain();
      f.failures.set("RELEASE SAVEPOINT fsqlite_sdk_1", "first release failed");
      f.release("RELEASE SAVEPOINT fsqlite_sdk_1");
      f.failures.clear();
      await child.settled;
      expect((rejected(child) as Error).message).toContain("first release failed");
      await parent.execute("RECOVERED");
    });
    expect(f.log()).toEqual([
      "BEGIN", "SAVEPOINT fsqlite_sdk_1", "CHILD", "RELEASE SAVEPOINT fsqlite_sdk_1",
      "ROLLBACK TO SAVEPOINT fsqlite_sdk_1", "RELEASE SAVEPOINT fsqlite_sdk_1", "RECOVERED", "COMMIT",
    ]);
    await f.db.close();
  });

  it("retains the child, rollback and cleanup errors even when the parent catches the child", async () => {
    const f = await fixture();
    f.failures.set("ROLLBACK TO SAVEPOINT fsqlite_sdk_1", "rollback-to failed");
    const cause = new Error("child failed");
    const cleanupError = new Error("terminate failed");
    f.worker.onTerminate = () => { throw cleanupError; };
    const transaction = observe(f.db.transaction(async (parent) => {
      await parent.transaction(() => { throw cause; }).catch(() => {});
    }));
    await transaction.settled;
    const error = rejected(transaction);
    expect(error instanceof AggregateError).toBe(true);
    expect((error as AggregateError).cause).toBe(cause);
    expect((error as AggregateError).errors[0]).toBe(cause);
    expect(((error as AggregateError).errors[1] as Error).message).toContain("rollback-to failed");
    expect((error as AggregateError).errors[2]).toBe(cleanupError);
    expect(f.worker.terminateCount).toBe(1);
    expect(f.log()).toEqual(["BEGIN", "SAVEPOINT fsqlite_sdk_1", "ROLLBACK TO SAVEPOINT fsqlite_sdk_1"]);
  });
});