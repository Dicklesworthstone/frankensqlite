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
    requests() { return worker.requests.map((request) => "sql" in request ? request.sql : request.kind); },
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
      "init", "execute-batch", "prepare", "statement-execute-many", "statement-finalize", "execute-batch",
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
