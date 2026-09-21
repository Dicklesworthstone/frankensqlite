import { describe, expect, it } from "vitest";
import { validateBulkSql } from "../src/bulk";
import type { CoreDatabaseHandle, CorePreparedStatementHandle } from "../src/connection";
import { WorkerConnectionHost } from "../src/connection";
import type { ExecuteManyRequest, QueryResult, SqlScalar, WorkerResponse } from "../src/protocol";
import { MAX_EXECUTE_MANY_ROWS } from "../src/protocol";

const SQL = "INSERT INTO items(v) VALUES (?)";
const emptyResult: QueryResult = {
  columns: [],
  columnCount: 0,
  columnTypes: [],
  rows: [],
  rowArrays: [],
  changes: 0,
};

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

interface Faults {
  begin?: boolean;
  row?: number;
  rollback?: boolean;
  release?: number;
  free?: boolean;
  close?: boolean;
  resultColumns?: number;
  wholeTransactionRollback?: boolean;
  count?: number;
}

async function fixture(faults: Faults = {}) {
  const events: string[] = [];
  const bindings: unknown[][] = [];
  let rows: unknown[][] = [["before"]];
  const snapshots: { name: string; rows: unknown[][] }[] = [];
  let frees = 0;
  let prepares = 0;
  let releases = 0;
  let rowGate: ReturnType<typeof deferred> | undefined;
  let blockedRow = 0;
  const started = deferred();
  const boundaries = new Map<
    string,
    { gate: ReturnType<typeof deferred>; started: ReturnType<typeof deferred> }
  >();
  async function waitBoundary(name: string): Promise<void> {
    const boundary = boundaries.get(name);
    boundary?.started.resolve();
    await boundary?.gate.promise;
  }
  const statement: CorePreparedStatementHandle = {
    sql: SQL,
    columnCount: faults.resultColumns ?? 0,
    columnNames: () => [],
    free() {
      events.push("free");
      frees += 1;
      if (faults.free) throw new Error("free failed");
    },
    async execute() {
      throw new Error("unbound execution reused prior bindings");
    },
    async executeWithParams(params) {
      events.push("row");
      const index = bindings.length;
      bindings.push(params);
      if (rowGate !== undefined && index === blockedRow) {
        started.resolve();
        await rowGate.promise;
      }
      if (faults.row === index) {
        if (faults.wholeTransactionRollback) {
          rows = snapshots[0]?.rows ?? [];
          snapshots.length = 0;
        }
        throw Object.assign(new Error("constraint failed"), {
          code: "SQLITE_CONSTRAINT",
          sqliteCode: 19,
          extendedCode: 2067,
        });
      }
      if (params[0] !== "skip") rows.push([...params]);
      return faults.count ?? (params[0] === "skip" ? 0 : params[0] === "two" ? 2 : 1);
    },
    async query() {
      return emptyResult;
    },
    async queryWithParams() {
      return emptyResult;
    },
  };
  const db: CoreDatabaseHandle = {
    path: ":memory:",
    close() {
      events.push("close");
      if (faults.close) throw new Error("close failed");
    },
    free() {
      events.push("database.free");
    },
    async execute() {
      events.push("execute");
      return 1;
    },
    async executeWithParams() {
      return 1;
    },
    async executeBatch(sql) {
      events.push(sql);
      const name = sql.split(" ").at(-1)!;
      if (sql.startsWith("SAVEPOINT ")) {
        await waitBoundary("savepoint");
        if (faults.begin) throw new Error("begin failed");
        snapshots.push({ name, rows: rows.map((row) => [...row]) });
      } else if (sql.startsWith("ROLLBACK TO ")) {
        await waitBoundary("rollback");
        if (faults.rollback) throw new Error("rollback failed");
        const snapshot = snapshots.at(-1);
        if (snapshot?.name !== name) throw new Error("savepoint missing");
        rows = snapshot.rows.map((row) => [...row]);
      } else if (sql.startsWith("RELEASE ")) {
        await waitBoundary("release");
        releases += 1;
        if (faults.release === releases) throw new Error("release failed");
        if (snapshots.at(-1)?.name !== name) throw new Error("savepoint missing");
        snapshots.pop();
      }
    },
    async query() {
      events.push("query");
      return emptyResult;
    },
    async queryWithParams() {
      return emptyResult;
    },
    async prepare() {
      events.push("prepare");
      prepares += 1;
      await waitBoundary("prepare");
      return statement;
    },
    async export() {
      return Uint8Array.of(1);
    },
  };
  const host = new WorkerConnectionHost({
    async load() {
      return {
        FrankenDB: {
          async create() {
            return db;
          },
          async import() {
            return db;
          },
        },
      };
    },
  });
  await host.handle({ kind: "init", requestId: 1, config: {} });
  return {
    host,
    db,
    events,
    bindings,
    rows: () => rows,
    frees: () => frees,
    prepares: () => prepares,
    block(index = 0) {
      blockedRow = index;
      rowGate = deferred();
      return { ...rowGate, started: started.promise };
    },
    blockBoundary(name: string) {
      const gate = deferred();
      const started = deferred();
      boundaries.set(name, { gate, started });
      return { ...gate, started: started.promise };
    },
    run(parameterSets: SqlScalar[][] = [[1], [2], [3]], sql = SQL) {
      return host.handle({ kind: "execute-many", requestId: 2, sql, parameterSets });
    },
  };
}

function error(response: WorkerResponse) {
  expect(response.kind).toBe("error");
  if (response.kind !== "error") throw new Error("expected worker error");
  return response.error;
}

describe("atomic bulk execution", () => {
  it("prepares once, binds each row and returns exact per-execution counts", async () => {
    const f = await fixture();
    expect(await f.run([[1], ["skip"], ["two"]])).toEqual({
      kind: "execute-many-result",
      requestId: 2,
      data: { executions: 3, changes: 3, changesPerExecution: [1, 0, 2] },
    });
    expect(f.prepares()).toBe(1);
    expect(f.frees()).toBe(1);
    expect(f.events).toEqual([
      "prepare",
      "SAVEPOINT fsqlite_bulk_1",
      "row",
      "row",
      "row",
      "free",
      "RELEASE SAVEPOINT fsqlite_bulk_1",
    ]);
  });

  it("does not prepare or start a transaction for an empty input", async () => {
    const f = await fixture();
    expect(await f.run([])).toEqual({
      kind: "execute-many-result",
      requestId: 2,
      data: { executions: 0, changes: 0, changesPerExecution: [] },
    });
    expect(f.events).toEqual([]);
  });

  for (const row of [0, 1, 2]) {
    it(`rolls back the complete batch when parameter set ${row} fails`, async () => {
      const f = await fixture({ row });
      const failure = error(await f.run());
      expect(failure.batchIndex).toBe(row);
      expect(failure.code).toBe("SQLITE_CONSTRAINT");
      expect(failure.sqliteCode).toBe(19);
      expect(failure.extendedCode).toBe(2067);
      expect(failure.cause?.message).toBe("constraint failed");
      expect(f.rows()).toEqual([["before"]]);
      expect(f.frees()).toBe(1);
      expect(f.events.slice(-2)).toEqual([
        "ROLLBACK TO SAVEPOINT fsqlite_bulk_1",
        "RELEASE SAVEPOINT fsqlite_bulk_1",
      ]);
      expect((await f.host.handle({ kind: "query", requestId: 3, sql: "SELECT 1" })).kind).toBe(
        "query-result",
      );
    });
  }

  it("preserves the caller's outer savepoint on ordinary batch failure", async () => {
    const f = await fixture({ row: 1 });
    await f.db.executeBatch("SAVEPOINT outer");
    error(await f.run());
    expect(f.rows()).toEqual([["before"]]);
    await f.db.executeBatch("RELEASE SAVEPOINT outer");
  });

  it("binds null, bigint and blobs without string interpolation or stale binds", async () => {
    const f = await fixture();
    const sets: SqlScalar[][] = [[null, 9007199254740993n, Uint8Array.of(0, 255)], []];
    expect((await f.run(sets)).kind).toBe("execute-many-result");
    expect(f.bindings).toEqual(sets);
  });

  it("borrows a prepared handle without freeing it", async () => {
    const f = await fixture();
    const prepared = await f.host.handle({ kind: "prepare", requestId: 2, sql: SQL });
    if (prepared.kind !== "prepare-result") throw new Error("prepare failed");
    expect(
      (
        await f.host.handle({
          kind: "statement-execute-many",
          requestId: 3,
          statementId: prepared.data.statementId,
          parameterSets: [[1], [2]],
        })
      ).kind,
    ).toBe("execute-many-result");
    expect(f.prepares()).toBe(1);
    expect(f.frees()).toBe(0);
    await f.host.handle({
      kind: "statement-finalize",
      requestId: 4,
      statementId: prepared.data.statementId,
    });
    expect(f.frees()).toBe(1);
  });

  it("rejects RETURNING before opening the savepoint and frees the owned statement", async () => {
    const f = await fixture({ resultColumns: 1 });
    expect(error(await f.run()).code).toBe("ERR_FSQLITE_BULK_INPUT");
    expect(f.events).toEqual(["prepare", "free"]);
  });

  it("does not rollback if savepoint creation fails", async () => {
    const f = await fixture({ begin: true });
    expect(error(await f.run()).batchIndex).toBeUndefined();
    expect(f.events).toEqual(["prepare", "SAVEPOINT fsqlite_bulk_1", "free"]);
  });

  it("rolls back an unsuccessful release instead of returning a success count", async () => {
    const f = await fixture({ release: 1 });
    const failure = error(await f.run());
    expect(failure.batchIndex).toBeUndefined();
    expect(f.rows()).toEqual([["before"]]);
    expect(f.events.slice(-3)).toEqual([
      "RELEASE SAVEPOINT fsqlite_bulk_1",
      "ROLLBACK TO SAVEPOINT fsqlite_bulk_1",
      "RELEASE SAVEPOINT fsqlite_bulk_1",
    ]);
  });

  for (const faults of [
    { row: 1, rollback: true },
    { row: 1, release: 1 },
    { row: 1, wholeTransactionRollback: true },
  ]) {
    it(`poisons the host after failed recovery: ${JSON.stringify(faults)}`, async () => {
      const f = await fixture(faults);
      const failure = error(await f.run());
      expect(failure.code).toBe("ERR_FSQLITE_BULK_CONNECTION_UNUSABLE");
      expect(failure.transient).toBe(false);
      expect(failure.cleanupErrors?.length).toBe(1);
      expect(failure.cause?.code).toBe("SQLITE_CONSTRAINT");
      const eventsBefore = [...f.events];
      error(
        await f.host.handle({
          kind: "execute",
          requestId: 3,
          sql: "INSERT INTO items VALUES (99)",
        }),
      );
      expect(f.events).toEqual(eventsBefore);
      expect(f.events.slice(-2)).toEqual(["close", "database.free"]);
      expect((await f.host.handle({ kind: "close", requestId: 4 })).kind).toBe("close-result");
      if (faults.rollback)
        expect(f.events.includes("RELEASE SAVEPOINT fsqlite_bulk_1")).toBe(false);
    });
  }

  it("rolls back before reporting a statement-free failure", async () => {
    const f = await fixture({ free: true });
    expect(error(await f.run()).code).toBe("ERR_FSQLITE_BULK_CONNECTION_UNUSABLE");
    expect(f.rows()).toEqual([["before"]]);
    expect(f.frees()).toBe(1);
  });

  it("preserves row, free, rollback and database-close errors", async () => {
    const f = await fixture({ row: 0, free: true, rollback: true, close: true });
    const failure = error(await f.run());
    expect(failure.cause?.message).toBe("constraint failed");
    expect(failure.cleanupErrors?.map((item) => item.message)).toEqual([
      "free failed",
      "rollback failed",
      "close failed",
    ]);
    expect(f.events.at(-1)).toBe("database.free");
  });

  it("holds FIFO ownership until the complete batch has finished", async () => {
    const f = await fixture();
    const gate = f.block();
    const bulk = f.run();
    await gate.started;
    const query = f.host.handle({ kind: "query", requestId: 3, sql: "SELECT 1" });
    const close = f.host.handle({ kind: "close", requestId: 4 });
    await Promise.resolve();
    const before = [...f.events];
    gate.resolve();
    const responses = await Promise.all([bulk, query, close]);
    expect(before).toEqual(["prepare", "SAVEPOINT fsqlite_bulk_1", "row"]);
    expect(responses.map((response) => response.kind)).toEqual([
      "execute-many-result",
      "query-result",
      "close-result",
    ]);
    expect(f.events.slice(-4)).toEqual([
      "RELEASE SAVEPOINT fsqlite_bulk_1",
      "query",
      "close",
      "database.free",
    ]);
  });

  it("rejects oversized or malformed input before preparing or writing", async () => {
    const f = await fixture();
    expect(
      error(await f.run(Array.from({ length: MAX_EXECUTE_MANY_ROWS + 1 }, () => [1]))).code,
    ).toBe("ERR_FSQLITE_BULK_INPUT");
    const malformed = { kind: "execute-many", requestId: 3, sql: SQL, parameterSets: [[1], null] };
    expect(error(await f.host.handle(malformed as unknown as ExecuteManyRequest)).batchIndex).toBe(
      1,
    );
    expect(f.events).toEqual([]);
  });

  for (const count of [-1, 1.5, Number.NaN, Number.MAX_SAFE_INTEGER]) {
    it(`rolls back invalid or overflowing affected-row counts (${count})`, async () => {
      const f = await fixture({ count });
      expect(error(await f.run()).code).toBe("ERR_FSQLITE_BULK_INPUT");
      expect(f.rows()).toEqual([["before"]]);
    });
  }
});

describe("bulk SQL boundary", () => {
  for (const sql of [
    "BEGIN",
    "COMMIT",
    "ROLLBACK",
    "SAVEPOINT s",
    "RELEASE s",
    "PRAGMA journal_mode=OFF",
    "CREATE TABLE t(v)",
    "SELECT 1",
    "-- empty",
    "/* empty */",
    "",
    "INSERT INTO t VALUES(1); COMMIT",
    "INSERT INTO t VALUES(1); -- tail\n DELETE FROM t",
    "INSERT INTO t VALUES('unterminated)",
    "INSERT INTO t VALUES(1) /* unterminated",
    "INSERT INTO t VALUES(1)\0; COMMIT",
  ]) {
    it(`rejects unsafe batch boundary ${JSON.stringify(sql)}`, async () => {
      const f = await fixture();
      expect(error(await f.run([[1]], sql)).code).toBe("ERR_FSQLITE_BULK_INPUT");
      expect(f.events).toEqual([]);
    });
  }

  for (const sql of [
    "INSERT INTO t VALUES('a;''b'); -- trailing comment",
    'UPDATE "a;b" SET "v" = ?',
    "DELETE FROM `a;b` WHERE v = ?",
    "REPLACE INTO [a;b] VALUES(?) /* ; COMMIT */;",
    "/* leading ; */ -- ignored\n WITH x(v) AS (SELECT ?) INSERT INTO t SELECT v FROM x;",
  ]) {
    it(`accepts quoted/commented DML ${sql}`, () => {
      validateBulkSql(sql);
    });
  }
});

function cancel(host: WorkerConnectionHost, targetRequestId = 2, requestId = 90) {
  return host.handle({ kind: "cancel-bulk", requestId, targetRequestId });
}

function cancellable(host: WorkerConnectionHost, requestId = 2) {
  return host.handle({
    kind: "execute-many",
    requestId,
    sql: SQL,
    parameterSets: [[1], [2], [3]],
    cancellable: true,
  });
}

describe("bulk cancellation and commit boundary", () => {
  it("cancels a queued batch before any prepare or savepoint", async () => {
    const f = await fixture();
    const pending = cancellable(f.host);
    expect(await cancel(f.host)).toEqual({
      kind: "cancel-bulk-result",
      requestId: 90,
      accepted: true,
    });
    expect(error(await pending).code).toBe("ERR_FSQLITE_BULK_CANCELLED");
    expect(f.events).toEqual([]);
    expect(await cancel(f.host)).toEqual({
      kind: "cancel-bulk-result",
      requestId: 90,
      accepted: false,
    });
  });

  for (const blockedRow of [0, 2]) {
    it(`finishes the active row ${blockedRow} and rolls back the entire batch`, async () => {
      const f = await fixture();
      const gate = f.block(blockedRow);
      const pending = cancellable(f.host);
      let settled = false;
      void pending.then(() => {
        settled = true;
      });
      await gate.started;
      expect(await cancel(f.host)).toEqual({
        kind: "cancel-bulk-result",
        requestId: 90,
        accepted: true,
      });
      expect(settled).toBe(false);
      expect(f.frees()).toBe(0);
      gate.resolve();
      expect(error(await pending).code).toBe("ERR_FSQLITE_BULK_CANCELLED");
      expect(f.rows()).toEqual([["before"]]);
      expect(f.frees()).toBe(1);
      expect(f.events.slice(-2)).toEqual([
        "ROLLBACK TO SAVEPOINT fsqlite_bulk_1",
        "RELEASE SAVEPOINT fsqlite_bulk_1",
      ]);
      expect((await f.run([[4]])).kind).toBe("execute-many-result");
      expect(f.rows()).toEqual([["before"], [4]]);
    });
  }

  it("does not settle the batch until the cancellation rollback completes", async () => {
    const f = await fixture();
    const row = f.block();
    const rollback = f.blockBoundary("rollback");
    const pending = cancellable(f.host);
    let settled = false;
    void pending.then(() => {
      settled = true;
    });
    await row.started;
    await cancel(f.host);
    row.resolve();
    await rollback.started;
    expect(settled).toBe(false);
    expect(f.events.at(-1)).toBe("ROLLBACK TO SAVEPOINT fsqlite_bulk_1");
    rollback.resolve();
    expect(error(await pending).code).toBe("ERR_FSQLITE_BULK_CANCELLED");
    expect(f.rows()).toEqual([["before"]]);
  });

  it("frees a statement prepared during cancellation without starting a savepoint", async () => {
    const f = await fixture();
    const gate = f.blockBoundary("prepare");
    const pending = cancellable(f.host);
    await gate.started;
    await cancel(f.host);
    gate.resolve();
    expect(error(await pending).code).toBe("ERR_FSQLITE_BULK_CANCELLED");
    expect(f.events).toEqual(["prepare", "free"]);
  });

  it("rolls back when cancellation arrives during savepoint creation", async () => {
    const f = await fixture();
    const gate = f.blockBoundary("savepoint");
    const pending = cancellable(f.host);
    await gate.started;
    await cancel(f.host);
    gate.resolve();
    expect(error(await pending).code).toBe("ERR_FSQLITE_BULK_CANCELLED");
    expect(f.bindings.length).toBe(0);
    expect(f.rows()).toEqual([["before"]]);
    expect(f.events.includes("ROLLBACK TO SAVEPOINT fsqlite_bulk_1")).toBe(true);
  });

  it("refuses late cancellation once RELEASE has been dispatched", async () => {
    const f = await fixture();
    const gate = f.blockBoundary("release");
    const pending = cancellable(f.host);
    await gate.started;
    expect(await cancel(f.host)).toEqual({
      kind: "cancel-bulk-result",
      requestId: 90,
      accepted: false,
    });
    gate.resolve();
    expect((await pending).kind).toBe("execute-many-result");
    expect(f.rows()).toEqual([["before"], [1], [2], [3]]);
    expect(f.events.some((event) => event.startsWith("ROLLBACK"))).toBe(false);
  });

  it("preserves a statement error that races with cancellation", async () => {
    const f = await fixture({ row: 0 });
    const gate = f.block();
    const pending = cancellable(f.host);
    await gate.started;
    await cancel(f.host);
    gate.resolve();
    expect(error(await pending).code).toBe("SQLITE_CONSTRAINT");
    expect(f.rows()).toEqual([["before"]]);
  });

  it("invalidates the host if rollback of a cancelled batch fails", async () => {
    const f = await fixture({ rollback: true });
    const gate = f.block();
    const pending = cancellable(f.host);
    await gate.started;
    await cancel(f.host);
    gate.resolve();
    const failure = error(await pending);
    expect(failure.code).toBe("ERR_FSQLITE_BULK_CONNECTION_UNUSABLE");
    expect(failure.cause?.code).toBe("ERR_FSQLITE_BULK_CANCELLED");
    expect(failure.cleanupErrors?.map((item) => item.message)).toEqual(["rollback failed"]);
    expect(f.events.slice(-2)).toEqual(["close", "database.free"]);
    expect(error(await f.run([[4]])).code).toBe("ERR_FSQLITE_BULK_CONNECTION_UNUSABLE");
  });

  it("targets one queued batch without cancelling its predecessor or successor", async () => {
    const f = await fixture();
    const gate = f.block();
    const first = f.run([[11]]);
    await gate.started;
    const second = cancellable(f.host, 3);
    const third = f.host.handle({
      kind: "execute-many",
      requestId: 4,
      sql: SQL,
      parameterSets: [[44]],
    });
    await cancel(f.host, 3);
    gate.resolve();
    expect((await first).kind).toBe("execute-many-result");
    expect(error(await second).code).toBe("ERR_FSQLITE_BULK_CANCELLED");
    expect((await third).kind).toBe("execute-many-result");
    expect(f.rows()).toEqual([["before"], [11], [44]]);
  });

  it("does not cancel noncancellable, unknown or completed operations", async () => {
    const f = await fixture();
    const gate = f.block();
    const pending = f.run();
    await gate.started;
    expect(await cancel(f.host)).toEqual({
      kind: "cancel-bulk-result",
      requestId: 90,
      accepted: false,
    });
    expect(await cancel(f.host, 123)).toEqual({
      kind: "cancel-bulk-result",
      requestId: 90,
      accepted: false,
    });
    gate.resolve();
    expect((await pending).kind).toBe("execute-many-result");
    expect(await cancel(f.host)).toEqual({
      kind: "cancel-bulk-result",
      requestId: 90,
      accepted: false,
    });
  });

  it("rejects duplicate active cancellation ids without replacing the first token", async () => {
    const f = await fixture();
    const pending = cancellable(f.host);
    expect(error(await cancellable(f.host)).message).toBe("Duplicate active bulk request id");
    expect(await cancel(f.host)).toEqual({
      kind: "cancel-bulk-result",
      requestId: 90,
      accepted: true,
    });
    expect(error(await pending).code).toBe("ERR_FSQLITE_BULK_CANCELLED");
  });

  it("keeps a cancelled prepared handle reusable after successful rollback", async () => {
    const f = await fixture();
    const prepared = await f.host.handle({ kind: "prepare", requestId: 7, sql: SQL });
    if (prepared.kind !== "prepare-result") throw new Error("prepare failed");
    const gate = f.block();
    const pending = f.host.handle({
      kind: "statement-execute-many",
      requestId: 2,
      statementId: prepared.data.statementId,
      parameterSets: [[1]],
      cancellable: true,
    });
    await gate.started;
    await cancel(f.host);
    gate.resolve();
    expect(error(await pending).code).toBe("ERR_FSQLITE_BULK_CANCELLED");
    expect(f.frees()).toBe(0);
    expect(
      (
        await f.host.handle({
          kind: "statement-execute-many",
          requestId: 3,
          statementId: prepared.data.statementId,
          parameterSets: [[4]],
        })
      ).kind,
    ).toBe("execute-many-result");
    expect(f.rows()).toEqual([["before"], [4]]);
  });
});
