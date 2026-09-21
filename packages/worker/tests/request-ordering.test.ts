import { describe, expect, it } from "vitest";
import type { CoreDatabaseHandle, CorePreparedStatementHandle } from "../src/connection";
import { WorkerConnectionHost } from "../src/connection";
import type { QueryResult } from "../src/protocol";

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((yes, no) => {
    resolve = yes;
    reject = no;
  });
  return { promise, resolve, reject };
}

function fixture() {
  const events: string[] = [];
  const gates = new Map<string, ReturnType<typeof deferred<void>>>();
  const starts = new Map<string, ReturnType<typeof deferred<void>>>();
  const result: QueryResult = {
    columns: ["value"],
    columnCount: 1,
    columnTypes: ["integer"],
    rows: [{ value: 1 }],
    rowArrays: [[1]],
    changes: 0,
  };
  async function step(name: string): Promise<void> {
    events.push(`${name}:start`);
    starts.get(name)?.resolve();
    await gates.get(name)?.promise;
    events.push(`${name}:end`);
  }
  const statement: CorePreparedStatementHandle = {
    sql: "SELECT 1",
    columnCount: 1,
    columnNames: () => ["value"],
    free: () => {
      events.push("statement:free");
    },
    execute: async () => {
      await step("statement-execute");
      return 1;
    },
    executeWithParams: async () => {
      await step("statement-execute");
      return 1;
    },
    query: async () => {
      await step("statement-query");
      return result;
    },
    queryWithParams: async () => {
      await step("statement-query");
      return result;
    },
  };
  const db: CoreDatabaseHandle = {
    path: ":memory:",
    close: () => {
      events.push("database:close");
    },
    free: () => {
      events.push("database:free");
    },
    execute: async () => {
      await step("execute");
      return 1;
    },
    executeWithParams: async () => {
      await step("execute");
      return 1;
    },
    executeBatch: async () => {
      await step("batch");
    },
    query: async () => {
      await step("query");
      return result;
    },
    queryWithParams: async () => {
      await step("query");
      return result;
    },
    prepare: async () => {
      await step("prepare");
      return statement;
    },
    export: async () => {
      await step("export");
      return Uint8Array.of(1);
    },
  };
  const host = new WorkerConnectionHost({
    async load() {
      return {
        FrankenDB: {
          async create() {
            await step("create");
            return db;
          },
          async import() {
            await step("import");
            return db;
          },
        },
      };
    },
  });
  return {
    host,
    events,
    block(name: string) {
      const gate = deferred<void>();
      const started = deferred<void>();
      gates.set(name, gate);
      starts.set(name, started);
      return { ...gate, started: started.promise };
    },
    async init() {
      expect((await host.handle({ kind: "init", requestId: 1, config: {} })).kind).toBe("ready");
      events.length = 0;
    },
  };
}

describe("worker request ownership", () => {
  it("queues requests behind asynchronous initialization", async () => {
    const { host, events, block } = fixture();
    const gate = block("create");
    const init = host.handle({ kind: "init", requestId: 1, config: {} });
    const query = host.handle({ kind: "query", requestId: 2, sql: "SELECT 1" });
    await gate.started;
    gate.resolve();
    expect((await init).kind).toBe("ready");
    expect((await query).kind).toBe("query-result");
    expect(events).toEqual(["create:start", "create:end", "query:start", "query:end"]);
  });

  it("does not close a database while a query is suspended", async () => {
    const f = fixture();
    await f.init();
    const gate = f.block("query");
    const query = f.host.handle({ kind: "query", requestId: 2, sql: "SELECT 1" });
    await gate.started;
    const close = f.host.handle({ kind: "close", requestId: 3 });
    const beforeRelease = [...f.events];
    gate.resolve();
    await Promise.all([query, close]);
    expect(beforeRelease).toEqual(["query:start"]);
    expect(f.events).toEqual(["query:start", "query:end", "database:close", "database:free"]);
    expect((await f.host.handle({ kind: "query", requestId: 4, sql: "SELECT 1" })).kind).toBe(
      "error",
    );
  });

  it("keeps a prepared statement alive until its pending query settles", async () => {
    const f = fixture();
    await f.init();
    const prepared = await f.host.handle({ kind: "prepare", requestId: 2, sql: "SELECT 1" });
    if (prepared.kind !== "prepare-result") throw new Error("prepare failed");
    f.events.length = 0;
    const gate = f.block("statement-query");
    const query = f.host.handle({
      kind: "statement-query",
      requestId: 3,
      statementId: prepared.data.statementId,
    });
    await gate.started;
    const finalize = f.host.handle({
      kind: "statement-finalize",
      requestId: 4,
      statementId: prepared.data.statementId,
    });
    const beforeRelease = [...f.events];
    gate.resolve();
    await Promise.all([query, finalize]);
    expect(beforeRelease).toEqual(["statement-query:start"]);
    expect(f.events).toEqual(["statement-query:start", "statement-query:end", "statement:free"]);
  });

  it("continues in FIFO order after a rejected operation", async () => {
    const f = fixture();
    await f.init();
    const gate = f.block("execute");
    const execute = f.host.handle({ kind: "execute", requestId: 2, sql: "FAIL" });
    await gate.started;
    const query = f.host.handle({ kind: "query", requestId: 3, sql: "SELECT 1" });
    const close = f.host.handle({ kind: "close", requestId: 4 });
    const beforeRelease = [...f.events];
    gate.reject(new Error("statement failed"));
    const responses = await Promise.all([execute, query, close]);
    expect(beforeRelease).toEqual(["execute:start"]);
    expect(responses.map((r) => [r.requestId, r.kind])).toEqual([
      [2, "error"],
      [3, "query-result"],
      [4, "close-result"],
    ]);
    expect(f.events).toEqual([
      "execute:start",
      "query:start",
      "query:end",
      "database:close",
      "database:free",
    ]);
  });

  it("recovers even when error serialization itself throws", async () => {
    const f = fixture();
    await f.init();
    const gate = f.block("execute");
    const execute = f.host.handle({ kind: "execute", requestId: 2, sql: "FAIL" });
    const rejection = execute.catch((error: unknown) => error);
    await gate.started;
    const query = f.host.handle({ kind: "query", requestId: 3, sql: "SELECT 1" });
    const failure = new Error("error getter failed");
    gate.reject({
      get code() {
        throw failure;
      },
    });
    expect(await rejection).toBe(failure);
    expect((await query).kind).toBe("query-result");
    expect(f.events).toEqual(["execute:start", "query:start", "query:end"]);
  });

  it("serializes reinitialization behind existing work", async () => {
    const f = fixture();
    await f.init();
    const gate = f.block("query");
    const query = f.host.handle({ kind: "query", requestId: 2, sql: "SELECT 1" });
    await gate.started;
    const init = f.host.handle({ kind: "init", requestId: 3, config: {} });
    gate.resolve();
    await Promise.all([query, init]);
    expect(f.events).toEqual([
      "query:start",
      "query:end",
      "database:close",
      "database:free",
      "create:start",
      "create:end",
    ]);
  });

  it("does not serialize independent database hosts with each other", async () => {
    const first = fixture();
    const second = fixture();
    await Promise.all([first.init(), second.init()]);
    const gate = first.block("query");
    const blocked = first.host.handle({ kind: "query", requestId: 2, sql: "SELECT 1" });
    await gate.started;
    const response = await second.host.handle({ kind: "query", requestId: 2, sql: "SELECT 1" });
    const beforeRelease = [...first.events];
    gate.resolve();
    await blocked;
    expect(response.kind).toBe("query-result");
    expect(beforeRelease).toEqual(["query:start"]);
  });
});
