import { describe, expect, it } from "vitest";

import { WorkerConnectionHost } from "../src/connection";
import type {
  CoreDatabaseConstructor,
  CoreDatabaseHandle,
  CoreModuleLoader,
  CorePreparedStatementHandle,
} from "../src/connection";
import type { QueryResult } from "../src/protocol";

class FakeStatement implements CorePreparedStatementHandle {
  readonly sql: string;
  readonly columnCount: number;
  readonly #rows: QueryResult;
  readonly #onFree: (() => void) | undefined;
  freeCount = 0;

  constructor(sql: string, rows: QueryResult, onFree?: () => void) {
    this.sql = sql;
    this.columnCount = rows.columnCount;
    this.#rows = rows;
    this.#onFree = onFree;
  }

  columnNames(): string[] {
    return [...this.#rows.columns];
  }

  free(): void {
    this.freeCount += 1;
    this.#onFree?.();
  }

  async execute(): Promise<number> {
    await Promise.resolve();
    return 1;
  }

  async executeWithParams(): Promise<number> {
    await Promise.resolve();
    return 1;
  }

  async query(): Promise<QueryResult> {
    await Promise.resolve();
    return this.#rows;
  }

  async queryWithParams(): Promise<QueryResult> {
    await Promise.resolve();
    return this.#rows;
  }
}

class FakeDatabase implements CoreDatabaseHandle {
  static importSettled = false;
  static lastCreated: FakeDatabase | null = null;
  readonly path: string;
  closeCount = 0;
  freeCount = 0;
  readonly statements: FakeStatement[] = [];
  readonly lifecycle: string[] = [];

  constructor(path = ":memory:") {
    this.path = path;
    FakeDatabase.lastCreated = this;
  }

  static async create(path?: string): Promise<CoreDatabaseHandle> {
    await Promise.resolve();
    return new FakeDatabase(path);
  }

  static async import(data: Uint8Array): Promise<CoreDatabaseHandle> {
    FakeDatabase.importSettled = false;
    await Promise.resolve();
    FakeDatabase.importSettled = true;
    if (data[0] === 0) {
      throw Object.assign(new Error("async import failed"), {
        code: "SQLITE_NOTADB",
        sqliteCode: 26,
      });
    }
    return new FakeDatabase(":memory:");
  }

  close(): void {
    this.closeCount += 1;
    this.lifecycle.push("database.close");
  }

  free(): void {
    this.freeCount += 1;
    this.lifecycle.push("database.free");
  }

  async execute(sql: string): Promise<number> {
    await Promise.resolve();
    if (sql === "FAIL") {
      throw Object.assign(new Error("async execute failed"), {
        code: "SQLITE_ERROR",
        sqliteCode: 1,
      });
    }
    return 1;
  }

  async executeBatch(): Promise<void> {
    await Promise.resolve();
  }

  async executeWithParams(): Promise<number> {
    await Promise.resolve();
    return 1;
  }

  async query(): Promise<QueryResult> {
    await Promise.resolve();
    return {
      columns: ["id", "name"],
      columnCount: 2,
      columnTypes: ["integer", "text"],
      rows: [{ id: 1, name: "alpha" }],
      rowArrays: [[1, "alpha"]],
      changes: 0,
    };
  }

  async queryWithParams(): Promise<QueryResult> {
    await Promise.resolve();
    return this.query();
  }

  async prepare(sql: string): Promise<CorePreparedStatementHandle> {
    const stmt = new FakeStatement(sql, await this.query(), () => {
      this.lifecycle.push("statement.free");
    });
    this.statements.push(stmt);
    return stmt;
  }

  async export(): Promise<Uint8Array> {
    await Promise.resolve();
    return Uint8Array.of(1, 2, 3, 4);
  }
}

const fakeLoader: CoreModuleLoader = {
  async load() {
    return {
      FrankenDB: FakeDatabase as unknown as CoreDatabaseConstructor,
    };
  },
};

describe("WorkerConnectionHost", () => {
  it("initializes a memory database and returns ready metadata", async () => {
    const host = new WorkerConnectionHost(fakeLoader);
    const response = await host.handle({
      kind: "init",
      requestId: 1,
      config: { dbName: "demo", persistence: "memory" },
    });

    expect(response.kind).toBe("ready");
    if (response.kind === "ready") {
      expect(response.data.persistence).toBe("memory");
      expect(response.data.path).toBe("demo");
    }
  });

  it("rejects persistence modes that are not implemented yet", async () => {
    const host = new WorkerConnectionHost(fakeLoader);
    const response = await host.handle({
      kind: "init",
      requestId: 1,
      config: { dbName: "demo", persistence: "opfs" },
    });

    expect(response.kind).toBe("error");
    if (response.kind === "error") {
      expect(response.error.code).toBe("ERR_FSQLITE_UNSUPPORTED_PERSISTENCE");
      expect(response.error.message).toContain("not implemented yet");
    }
  });

  it("serializes asynchronously rejected snapshot imports", async () => {
    const host = new WorkerConnectionHost(fakeLoader);
    const response = await host.handle({
      kind: "init",
      requestId: 1,
      config: {
        persistence: "memory",
        snapshot: Uint8Array.of(0),
      },
    });

    expect(response.kind).toBe("error");
    if (response.kind === "error") {
      expect(response.error.code).toBe("SQLITE_NOTADB");
      expect(response.error.sqliteCode).toBe(26);
      expect(response.error.message).toBe("async import failed");
    }
  });

  it("waits for snapshot import before publishing ready", async () => {
    const host = new WorkerConnectionHost(fakeLoader);
    FakeDatabase.importSettled = false;

    const responsePromise = host.handle({
      kind: "init",
      requestId: 1,
      config: {
        persistence: "memory",
        snapshot: Uint8Array.of(1),
      },
    });

    expect(FakeDatabase.importSettled).toBe(false);
    const response = await responsePromise;
    expect(FakeDatabase.importSettled).toBe(true);
    expect(response.kind).toBe("ready");
    if (response.kind === "ready") {
      expect(response.data.path).toBe(":memory:");
    }
  });

  it("supports prepare, query, export, and close lifecycle requests", async () => {
    const host = new WorkerConnectionHost(fakeLoader);
    await host.handle({
      kind: "init",
      requestId: 1,
      config: { persistence: "memory" },
    });

    const prepared = await host.handle({
      kind: "prepare",
      requestId: 2,
      sql: "SELECT id, name FROM demo",
    });
    expect(prepared.kind).toBe("prepare-result");
    if (prepared.kind !== "prepare-result") {
      return;
    }

    const queried = await host.handle({
      kind: "statement-query",
      requestId: 3,
      statementId: prepared.data.statementId,
    });
    expect(queried.kind).toBe("query-result");
    if (queried.kind === "query-result") {
      expect(queried.data.rows).toEqual([{ id: 1, name: "alpha" }]);
    }

    const exported = await host.handle({
      kind: "export",
      requestId: 4,
    });
    expect(exported.kind).toBe("export-result");
    if (exported.kind === "export-result") {
      expect([...exported.data]).toEqual([1, 2, 3, 4]);
    }

    const finalized = await host.handle({
      kind: "statement-finalize",
      requestId: 5,
      statementId: prepared.data.statementId,
    });
    expect(finalized.kind).toBe("statement-finalize-result");

    const closed = await host.handle({
      kind: "close",
      requestId: 6,
    });
    expect(closed.kind).toBe("close-result");
  });

  it("awaits database and statement operations before returning responses", async () => {
    const host = new WorkerConnectionHost(fakeLoader);
    await host.handle({
      kind: "init",
      requestId: 1,
      config: { persistence: "memory" },
    });

    const executed = await host.handle({
      kind: "execute",
      requestId: 2,
      sql: "INSERT INTO demo VALUES (1)",
    });
    expect(executed).toEqual({
      kind: "execute-result",
      requestId: 2,
      changes: 1,
    });

    const executedWithParams = await host.handle({
      kind: "execute",
      requestId: 3,
      sql: "INSERT INTO demo VALUES (?1)",
      params: [2],
    });
    expect(executedWithParams).toEqual({
      kind: "execute-result",
      requestId: 3,
      changes: 1,
    });

    const batchExecuted = await host.handle({
      kind: "execute-batch",
      requestId: 4,
      sql: "INSERT INTO demo VALUES (3); INSERT INTO demo VALUES (4);",
    });
    expect(batchExecuted).toEqual({
      kind: "execute-batch-result",
      requestId: 4,
    });

    const queried = await host.handle({
      kind: "query",
      requestId: 5,
      sql: "SELECT id, name FROM demo",
      params: [1],
    });
    expect(queried.kind).toBe("query-result");
    if (queried.kind === "query-result") {
      expect(queried.data.rows).toEqual([{ id: 1, name: "alpha" }]);
    }

    const prepared = await host.handle({
      kind: "prepare",
      requestId: 6,
      sql: "SELECT id, name FROM demo WHERE id = ?1",
    });
    expect(prepared.kind).toBe("prepare-result");
    if (prepared.kind !== "prepare-result") {
      return;
    }

    const statementExecuted = await host.handle({
      kind: "statement-execute",
      requestId: 7,
      statementId: prepared.data.statementId,
      params: [1],
    });
    expect(statementExecuted).toEqual({
      kind: "execute-result",
      requestId: 7,
      changes: 1,
    });

    const statementQueried = await host.handle({
      kind: "statement-query",
      requestId: 8,
      statementId: prepared.data.statementId,
      params: [1],
    });
    expect(statementQueried.kind).toBe("query-result");
    if (statementQueried.kind === "query-result") {
      expect(statementQueried.data.rows).toEqual([
        { id: 1, name: "alpha" },
      ]);
    }
  });

  it("serializes asynchronously rejected core operations", async () => {
    const host = new WorkerConnectionHost(fakeLoader);
    await host.handle({
      kind: "init",
      requestId: 1,
      config: { persistence: "memory" },
    });

    const response = await host.handle({
      kind: "execute",
      requestId: 2,
      sql: "FAIL",
    });

    expect(response.kind).toBe("error");
    if (response.kind === "error") {
      expect(response.error.code).toBe("SQLITE_ERROR");
      expect(response.error.sqliteCode).toBe(1);
      expect(response.error.message).toBe("async execute failed");
    }
  });

  it("frees finalized and remaining statements exactly once before database disposal", async () => {
    const host = new WorkerConnectionHost(fakeLoader);
    await host.handle({
      kind: "init",
      requestId: 1,
      config: { persistence: "memory" },
    });
    const db = FakeDatabase.lastCreated;
    expect(db).not.toBeNull();
    if (db === null) {
      return;
    }

    const first = await host.handle({
      kind: "prepare",
      requestId: 2,
      sql: "SELECT 1",
    });
    const second = await host.handle({
      kind: "prepare",
      requestId: 3,
      sql: "SELECT 2",
    });
    expect(first.kind).toBe("prepare-result");
    expect(second.kind).toBe("prepare-result");
    if (first.kind !== "prepare-result" || second.kind !== "prepare-result") {
      return;
    }

    const finalized = await host.handle({
      kind: "statement-finalize",
      requestId: 4,
      statementId: first.data.statementId,
    });
    expect(finalized.kind).toBe("statement-finalize-result");
    const finalizedAgain = await host.handle({
      kind: "statement-finalize",
      requestId: 5,
      statementId: first.data.statementId,
    });
    expect(finalizedAgain.kind).toBe("error");

    await host.handle({ kind: "close", requestId: 6 });
    await host.handle({ kind: "close", requestId: 7 });

    expect(db.statements[0]?.freeCount).toBe(1);
    expect(db.statements[1]?.freeCount).toBe(1);
    expect(db.closeCount).toBe(1);
    expect(db.freeCount).toBe(1);
    expect(db.lifecycle).toEqual([
      "statement.free",
      "statement.free",
      "database.close",
      "database.free",
    ]);
  });

  it("allows an admitted statement query to finish after immediate finalize", async () => {
    const host = new WorkerConnectionHost(fakeLoader);
    await host.handle({
      kind: "init",
      requestId: 1,
      config: { persistence: "memory" },
    });
    const db = FakeDatabase.lastCreated;
    expect(db).not.toBeNull();
    if (db === null) {
      return;
    }

    const prepared = await host.handle({
      kind: "prepare",
      requestId: 2,
      sql: "SELECT id, name FROM demo",
    });
    expect(prepared.kind).toBe("prepare-result");
    if (prepared.kind !== "prepare-result") {
      return;
    }

    const query = host.handle({
      kind: "statement-query",
      requestId: 3,
      statementId: prepared.data.statementId,
    });
    const finalized = await host.handle({
      kind: "statement-finalize",
      requestId: 4,
      statementId: prepared.data.statementId,
    });
    expect(finalized.kind).toBe("statement-finalize-result");
    expect(db.statements[0]?.freeCount).toBe(1);

    const queryResult = await query;
    expect(queryResult.kind).toBe("query-result");
    if (queryResult.kind === "query-result") {
      expect(queryResult.data.rows).toEqual([{ id: 1, name: "alpha" }]);
    }

    await host.handle({ kind: "close", requestId: 5 });
    expect(db.statements[0]?.freeCount).toBe(1);
  });

  it("allows an admitted database operation to finish after immediate close and free", async () => {
    const host = new WorkerConnectionHost(fakeLoader);
    await host.handle({
      kind: "init",
      requestId: 1,
      config: { persistence: "memory" },
    });
    const db = FakeDatabase.lastCreated;
    expect(db).not.toBeNull();
    if (db === null) {
      return;
    }

    const operation = host.handle({
      kind: "execute",
      requestId: 2,
      sql: "INSERT INTO demo VALUES (1)",
    });
    const closed = await host.handle({ kind: "close", requestId: 3 });
    expect(closed.kind).toBe("close-result");
    expect(db.closeCount).toBe(1);
    expect(db.freeCount).toBe(1);

    expect(await operation).toEqual({
      kind: "execute-result",
      requestId: 2,
      changes: 1,
    });

    await host.handle({ kind: "close", requestId: 4 });
    expect(db.closeCount).toBe(1);
    expect(db.freeCount).toBe(1);
  });
});
