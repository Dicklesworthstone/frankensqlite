import { describe, expect, it } from "vitest";
import type { SerializedFrankenError, WorkerRequest, WorkerResponse } from "@frankensqlite/worker";
import { FrankenDB, FrankenSQLiteError, MAX_EXECUTE_MANY_ROWS } from "../src/index";
import type { FrankenPreparedStatement, FrankenTransaction, SqlScalar } from "../src/index";
import { ControlledWorker, deferred, drain, observe, rejected } from "./helpers/controlled-worker";

const SQL = "INSERT INTO t(v) VALUES(?)";
const failure: SerializedFrankenError = {
  code: "SQLITE_CONSTRAINT", message: "bulk constraint failed", sqliteCode: 19,
  extendedCode: 2067, batchIndex: 2,
  cause: { code: "SQLITE_CONSTRAINT", message: "duplicate value", sqliteCode: 19, extendedCode: 2067 },
  cleanupErrors: [{ code: "SQLITE_IOERR", message: "cleanup failed", sqliteCode: 10 }],
};

async function fixture() {
  const worker = new ControlledWorker();
  let fail = false;
  let hold = false;
  const held: WorkerRequest[] = [];
  function response(request: WorkerRequest): WorkerResponse {
    const requestId = request.requestId;
    switch (request.kind) {
      case "init": return { kind: "ready", requestId, data: { path: ":memory:", persistence: "memory" } };
      case "execute-batch": return { kind: "execute-batch-result", requestId };
      case "transaction": return { kind: "transaction-result", requestId };
      case "execute-many":
      case "statement-execute-many": {
        if (fail) return { kind: "error", requestId, error: failure };
        const count = request.parameterSets.length;
        return { kind: "execute-many-result", requestId,
          data: { executions: count, changes: count, changesPerExecution: Array<number>(count).fill(1) } };
      }
      case "prepare": return { kind: "prepare-result", requestId,
        data: { statementId: `stmt-${requestId}`, sql: request.sql, columnCount: 0, columnNames: [] } };
      case "statement-finalize": return { kind: "statement-finalize-result", requestId };
      case "close": return { kind: "close-result", requestId };
      default: return { kind: "execute-result", requestId, changes: 1 };
    }
  }
  worker.onPost = (request) => {
    const copy = structuredClone(request);
    if (hold && (request.kind === "execute-many" || request.kind === "statement-execute-many")) {
      held.push(copy);
    } else {
      queueMicrotask(() => worker.reply(response(copy)));
    }
  };
  const db = await FrankenDB.open({ worker });
  return { db, worker,
    fail() { fail = true; },
    hold() { hold = true; },
    flush() { hold = false; for (const request of held.splice(0)) worker.reply(response(request)); },
    requests() { return worker.requests.map((request) => request.kind === "transaction"
      ? request.action.toUpperCase() : "sql" in request ? request.sql : request.kind); },
  };
}

describe("SDK atomic bulk execution", () => {
  it("sends a 10,000-row batch in one request, preserving typed results", async () => {
    const f = await fixture();
    const rows = Array.from({ length: MAX_EXECUTE_MANY_ROWS }, (_, i) => [i] as const);
    const result = await f.db.executeMany(SQL, rows);
    expect(result.executions).toBe(MAX_EXECUTE_MANY_ROWS);
    expect(result.changes).toBe(MAX_EXECUTE_MANY_ROWS);
    expect(result.changesPerExecution).toEqual(rows.map(() => 1));
    expect(f.worker.requests.map((request) => request.kind)).toEqual(["init", "execute-many"]);
  });

  it("supports empty batches without bypassing lifecycle admission", async () => {
    const f = await fixture();
    expect(await f.db.executeMany(SQL, [])).toEqual({ executions: 0, changes: 0, changesPerExecution: [] });
    await f.db.close();
    await expect(f.db.executeMany(SQL, [])).rejects.toThrow("disposed");
  });

  it("copies parameter arrays and preserves bigint, null and blobs", async () => {
    const f = await fixture();
    const values: SqlScalar[][] = [[9007199254740993n, null, Uint8Array.of(0, 255)]];
    const pending = f.db.executeMany(SQL, values);
    values[0]!.push("later");
    await pending;
    const request = f.worker.requests.at(-1);
    if (request?.kind !== "execute-many") throw new Error("wrong request");
    expect(request.parameterSets).toEqual([[9007199254740993n, null, Uint8Array.of(0, 255)]]);
  });

  it("uses callback ownership and the enclosing transaction boundary", async () => {
    const f = await fixture();
    await f.db.transaction(async (tx) => {
      expect((await tx.executeMany(SQL, [[1], [2]])).changes).toBe(2);
    });
    expect(f.requests()).toEqual(["init", "BEGIN", SQL, "COMMIT"]);
  });

  it("borrows a prepared statement and rejects reuse after finalization", async () => {
    const f = await fixture();
    const statement = await f.db.prepare(SQL);
    await statement.executeMany([[1], [2]]);
    await statement.execute([3]);
    await statement.finalize();
    await expect(statement.executeMany([])).rejects.toThrow("finalized");
    expect(f.worker.requests.map((request) => request.kind)).toEqual([
      "init", "prepare", "statement-execute-many", "statement-execute", "statement-finalize",
    ]);
  });

  it("finalizes scoped prepared batches and invalidates escaped handles", async () => {
    const f = await fixture();
    let escaped!: FrankenTransaction;
    let statement!: FrankenPreparedStatement;
    await f.db.transaction(async (tx) => {
      escaped = tx;
      statement = await tx.prepare(SQL);
      await statement.executeMany([[1]]);
    });
    await expect(escaped.executeMany(SQL, [[2]])).rejects.toThrow("finished");
    await expect(statement.executeMany([[2]])).rejects.toThrow("finished");
    expect(f.worker.requests.map((request) => request.kind)).toEqual([
      "init", "transaction", "prepare", "statement-execute-many", "statement-finalize", "transaction",
    ]);
  });

  it("refuses foreign database and prepared batches while a callback owns the connection", async () => {
    const f = await fixture();
    const outside = await f.db.prepare(SQL);
    await f.db.transaction(async (tx) => {
      await expect(f.db.executeMany(SQL, [[9]])).rejects.toThrow("owns this connection");
      await expect(outside.executeMany([[9]])).rejects.toThrow("owns this connection");
      await tx.executeMany(SQL, [[1]]);
    });
    expect(f.worker.requests.filter((r) => r.kind === "execute-many").length).toBe(1);
    expect(f.worker.requests.filter((r) => r.kind === "statement-execute-many").length).toBe(0);
  });

  it("permits only the deepest active transaction scope to submit batches", async () => {
    const f = await fixture();
    await f.db.transaction(async (parent) => {
      const statement = await parent.prepare(SQL);
      await parent.transaction(async (child) => {
        await expect(parent.executeMany(SQL, [[8]])).rejects.toThrow("owns this connection");
        await expect(statement.executeMany([[8]])).rejects.toThrow("owns this connection");
        await child.executeMany(SQL, [[1]]);
      });
      await parent.executeMany(SQL, [[2]]);
    });
    expect(f.worker.requests.filter((r) => r.kind === "execute-many").length).toBe(2);
  });

  it("preserves the row index, SQLite codes and structured cause/cleanup errors", async () => {
    const f = await fixture(); f.fail();
    const observed = observe(f.db.executeMany(SQL, [[1], [2], [1]]));
    await observed.settled;
    const error = rejected(observed);
    expect(error instanceof FrankenSQLiteError).toBe(true);
    if (!(error instanceof FrankenSQLiteError)) throw new Error("missing typed error");
    expect(error.batchIndex).toBe(2);
    expect(error.sqliteCode).toBe(19);
    expect(error.extendedCode).toBe(2067);
    expect(error.cause instanceof FrankenSQLiteError).toBe(true);
    if (!(error.cause instanceof FrankenSQLiteError)) throw new Error("missing cause");
    expect(error.cause.message).toBe("duplicate value");
    expect(error.cleanupErrors.map((item) => [item.code, item.message])).toEqual([["SQLITE_IOERR", "cleanup failed"]]);
  });

  it("drains an already-admitted batch before committing its callback", async () => {
    const f = await fixture(); f.hold();
    const entered = deferred<void>();
    const transaction = observe(f.db.transaction((tx) => {
      void tx.executeMany(SQL, [[1], [2]]);
      entered.resolve();
    }));
    await entered.promise; await drain();
    expect(transaction.outcome.status).toBe("pending");
    expect(f.requests()).toEqual(["init", "BEGIN", SQL]);
    f.flush(); await transaction.settled;
    expect(transaction.outcome.status).toBe("fulfilled");
    expect(f.requests().at(-1)).toBe("COMMIT");
  });

  it("rolls back when an unawaited batch fails instead of committing earlier work", async () => {
    const f = await fixture(); f.hold(); f.fail();
    const entered = deferred<void>();
    const transaction = observe(f.db.transaction((tx) => {
      void tx.executeMany(SQL, [[1], [2], [1]]);
      entered.resolve();
    }));
    await entered.promise; f.flush(); await transaction.settled;
    expect(rejected(transaction) instanceof FrankenSQLiteError).toBe(true);
    expect(f.requests().at(-1)).toBe("ROLLBACK");
  });

  it("rejects oversized and malformed batches before sending them", async () => {
    const f = await fixture();
    await expect(f.db.executeMany(SQL, Array.from({ length: MAX_EXECUTE_MANY_ROWS + 1 }, () => [1])))
      .rejects.toThrow("at most");
    const observed = observe(f.db.executeMany(SQL, [[1], null] as unknown as SqlScalar[][]));
    await observed.settled;
    const error = rejected(observed);
    if (!(error instanceof FrankenSQLiteError)) throw new Error("missing error");
    expect(error.batchIndex).toBe(1);
    expect(f.worker.requests.map((request) => request.kind)).toEqual(["init"]);
  });

  it("settles a clone failure without abandoning the next valid batch", async () => {
    const f = await fixture();
    const invalid = [[() => 1]] as unknown as SqlScalar[][];
    const observed = observe(f.db.executeMany(SQL, invalid));
    await observed.settled;
    expect(rejected(observed) instanceof Error).toBe(true);
    expect((await f.db.executeMany(SQL, [[1]])).changes).toBe(1);
  });
});

async function cancellationFixture() {
  const worker = new ControlledWorker();
  worker.onPost = (request) => {
    if (request.kind === "init") {
      queueMicrotask(() => worker.reply({ kind: "ready", requestId: request.requestId,
        data: { path: ":memory:", persistence: "memory" } }));
    } else if (request.kind === "cancel-bulk") {
      queueMicrotask(() => worker.reply({ kind: "cancel-bulk-result", requestId: request.requestId, accepted: true }));
    }
  };
  const db = await FrankenDB.open({ worker });
  const batchId = () => {
    const request = worker.requests.find((item) => item.kind === "execute-many");
    if (request === undefined) throw new Error("batch was not posted");
    return request.requestId;
  };
  return { db, worker, batchId,
    complete() { worker.reply({ kind: "execute-many-result", requestId: batchId(),
      data: { executions: 1, changes: 1, changesPerExecution: [1] } }); },
    cancelled() { worker.reply({ kind: "error", requestId: batchId(), error: {
      code: "ERR_FSQLITE_BULK_CANCELLED", message: "bulk execution was cancelled", transient: false,
    } }); },
  };
}

function trackedSignal() {
  const controller = new AbortController();
  const signal = controller.signal;
  const add = signal.addEventListener.bind(signal);
  const remove = signal.removeEventListener.bind(signal);
  let added = 0;
  let removed = 0;
  signal.addEventListener = ((...args: Parameters<typeof signal.addEventListener>) => {
    if (args[0] === "abort") added += 1;
    add(...args);
  }) as typeof signal.addEventListener;
  signal.removeEventListener = ((...args: Parameters<typeof signal.removeEventListener>) => {
    if (args[0] === "abort") removed += 1;
    remove(...args);
  }) as typeof signal.removeEventListener;
  return { controller, signal, counts: () => [added, removed] };
}

describe("SDK cooperative bulk cancellation", () => {
  it("rejects a pre-aborted signal without posting any batch or listener", async () => {
    const f = await cancellationFixture();
    const tracked = trackedSignal();
    tracked.controller.abort();
    await expect(f.db.executeMany(SQL, [[1]], { signal: tracked.signal })).rejects.toThrow("cancelled");
    expect(f.worker.requests.map((item) => item.kind)).toEqual(["init"]);
    expect(tracked.counts()).toEqual([0, 0]);
  });

  it("waits for the batch's rollback response, not the cancellation acknowledgement", async () => {
    const f = await cancellationFixture();
    const tracked = trackedSignal();
    const batch = observe(f.db.executeMany(SQL, [[1]], { signal: tracked.signal }));
    tracked.controller.abort();
    await drain();
    expect(batch.outcome.status).toBe("pending");
    expect(f.worker.requests.map((item) => item.kind)).toEqual(["init", "execute-many", "cancel-bulk"]);
    const request = f.worker.requests.at(-1);
    if (request?.kind !== "cancel-bulk") throw new Error("missing cancel request");
    expect(request.targetRequestId).toBe(f.batchId());
    f.cancelled();
    await batch.settled;
    const failure = rejected(batch);
    if (!(failure instanceof FrankenSQLiteError)) throw new Error("missing typed error");
    expect(failure.code).toBe("ERR_FSQLITE_BULK_CANCELLED");
    expect(tracked.counts()).toEqual([1, 1]);
  });

  it("preserves a successful commit when cancellation arrives too late", async () => {
    const f = await cancellationFixture();
    const controller = new AbortController();
    f.worker.onPost = (request) => {
      if (request.kind === "cancel-bulk") f.worker.reply({ kind: "cancel-bulk-result", requestId: request.requestId, accepted: false });
    };
    const batch = f.db.executeMany(SQL, [[1]], { signal: controller.signal });
    controller.abort();
    f.complete();
    expect(await batch).toEqual({ executions: 1, changes: 1, changesPerExecution: [1] });
  });

  it("removes the abort listener after success and ignores later aborts", async () => {
    const f = await cancellationFixture();
    const tracked = trackedSignal();
    const batch = f.db.executeMany(SQL, [[1]], { signal: tracked.signal });
    f.complete(); await batch;
    expect(tracked.counts()).toEqual([1, 1]);
    tracked.controller.abort();
    expect(f.worker.requests.map((item) => item.kind)).toEqual(["init", "execute-many"]);
  });

  it("removes listeners after a synchronous transport failure", async () => {
    const f = await cancellationFixture();
    const tracked = trackedSignal();
    f.worker.onPost = () => { throw new Error("clone failed"); };
    await expect(f.db.executeMany(SQL, [[1]], { signal: tracked.signal })).rejects.toThrow("clone failed");
    expect(tracked.counts()).toEqual([1, 1]);
    tracked.controller.abort();
    expect(f.worker.requests.map((item) => item.kind)).toEqual(["init", "execute-many"]);
  });

  it("posts cancellation after the batch when abort occurs inside postMessage", async () => {
    const f = await cancellationFixture();
    const controller = new AbortController();
    const prior = f.worker.onPost!;
    f.worker.onPost = (request) => {
      if (request.kind === "execute-many") controller.abort();
      prior(request);
    };
    const batch = observe(f.db.executeMany(SQL, [[1]], { signal: controller.signal }));
    expect(f.worker.requests.map((item) => item.kind)).toEqual(["init", "execute-many", "cancel-bulk"]);
    f.cancelled(); await batch.settled;
    expect(rejected(batch) instanceof FrankenSQLiteError).toBe(true);
  });

  it("does not turn a failed cancellation delivery into a false rollback claim", async () => {
    const f = await cancellationFixture();
    const controller = new AbortController();
    f.worker.onPost = (request) => {
      if (request.kind === "cancel-bulk") throw new Error("cancel transport unavailable");
    };
    const batch = observe(f.db.executeMany(SQL, [[1]], { signal: controller.signal }));
    controller.abort(); await drain();
    expect(batch.outcome.status).toBe("pending");
    f.complete(); await batch.settled;
    expect(batch.outcome.status).toBe("fulfilled");
  });

  it("allows cancellation of admitted work while close is queued", async () => {
    const f = await cancellationFixture();
    const controller = new AbortController();
    const batch = observe(f.db.executeMany(SQL, [[1]], { signal: controller.signal }));
    const close = f.db.close();
    controller.abort();
    expect(f.worker.requests.map((item) => item.kind)).toEqual(["init", "execute-many", "close", "cancel-bulk"]);
    await drain();
    expect(batch.outcome.status).toBe("pending");
    f.cancelled(); await batch.settled;
    const closeRequest = f.worker.requests.find((item) => item.kind === "close")!;
    f.worker.reply({ kind: "close-result", requestId: closeRequest.requestId });
    await close;
    expect(f.worker.terminateCount).toBe(1);
    expect(rejected(batch) instanceof FrankenSQLiteError).toBe(true);
  });

  it("settles the batch and removes its listener when the worker crashes", async () => {
    const f = await cancellationFixture();
    const tracked = trackedSignal();
    const batch = observe(f.db.executeMany(SQL, [[1]], { signal: tracked.signal }));
    tracked.controller.abort();
    f.worker.crash("worker stopped");
    await batch.settled;
    expect(String(rejected(batch))).toContain("worker stopped");
    expect(tracked.counts()).toEqual([1, 1]);
  });

  it("threads signals through prepared and managed transaction handles", async () => {
    const f = await fixture();
    const controller = new AbortController();
    const statement = await f.db.prepare(SQL);
    await statement.executeMany([[1]], { signal: controller.signal });
    await f.db.transaction(async (tx) => {
      await tx.executeMany(SQL, [[2]], { signal: controller.signal });
      const prepared = await tx.prepare(SQL);
      await prepared.executeMany([[3]], { signal: controller.signal });
    });
    const batches = f.worker.requests.filter((item) => item.kind === "execute-many" || item.kind === "statement-execute-many");
    expect(batches.map((item) => item.cancellable)).toEqual([true, true, true]);
  });

  it("does not bypass transaction ownership even with a pre-aborted signal", async () => {
    const f = await fixture();
    const controller = new AbortController(); controller.abort();
    await f.db.transaction(async () => {
      await expect(f.db.executeMany(SQL, [[1]], { signal: controller.signal })).rejects.toThrow("owns this connection");
    });
    expect(f.worker.requests.map((item) => item.kind)).toEqual(["init", "transaction", "transaction"]);
  });
});
