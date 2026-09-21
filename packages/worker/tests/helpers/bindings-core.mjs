// Independent native SQLite oracle with real parameter-slot metadata. Requires
// Python 3 and its system SQLite library, not used by production packages.
import { spawn } from "node:child_process";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";
import { WorkerConnectionHost } from "../../src/connection.ts";

export function sqliteBindingFixture(path = ":memory:") {
  const process = spawn(
    "python3",
    ["-u", fileURLToPath(new URL("./bindings-sqlite.py", import.meta.url)), path],
    { stdio: ["pipe", "pipe", "inherit"] },
  );
  const pending = new Map();
  let id = 0,
    finished = false;
  const requests = [];
  const exit = new Promise((resolve) =>
    process.once("exit", (code) => {
      finished = true;
      for (const p of pending.values()) p.reject(new Error(`SQLite reference exited ${code}`));
      pending.clear();
      resolve(code);
    }),
  );
  process.once("error", (error) => {
    for (const p of pending.values()) p.reject(error);
    pending.clear();
  });
  createInterface({ input: process.stdout }).on("line", (line) => {
    const message = JSON.parse(line);
    const p = pending.get(message.id);
    if (!p) return;
    pending.delete(message.id);
    if (message.error)
      p.reject(
        Object.assign(new Error(message.error.message), message.error, { code: "SQLITE_ERROR" }),
      );
    else p.resolve(message.result);
  });
  const rpc = (command) =>
    new Promise((resolve, reject) => {
      if (finished) {
        reject(new Error("SQLite reference is closed"));
        return;
      }
      const request = { id: ++id, ...command };
      pending.set(id, { resolve, reject });
      requests.push(command);
      process.stdin.write(
        JSON.stringify(request, (_key, value) =>
          typeof value === "bigint"
            ? { integer: String(value) }
            : value instanceof Uint8Array
              ? { blob: [...value] }
              : value,
        ) + "\n",
      );
    });
  function scalar(value) {
    if (value && typeof value === "object")
      return "integer" in value ? BigInt(value.integer) : Uint8Array.from(value.blob);
    return value;
  }
  function result(value) {
    const rowArrays = value.rowArrays.map((row) => row.map(scalar)),
      columns = value.columns;
    return {
      columns,
      columnCount: columns.length,
      columnTypes: [],
      rowArrays,
      rows: rowArrays.map((row) => Object.fromEntries(columns.map((name, i) => [name, row[i]]))),
      changes: value.changes,
    };
  }
  const core = {
    path,
    close() {},
    free() {},
    async execute(sql) {
      return (await rpc({ op: "execute", sql })).changes;
    },
    async executeWithParams(sql, params) {
      return (await rpc({ op: "execute", sql, params })).changes;
    },
    async executeBatch(sql) {
      await rpc({ op: "batch", sql });
    },
    async query(sql) {
      return result(await rpc({ op: "query", sql }));
    },
    async queryWithParams(sql, params) {
      return result(await rpc({ op: "query", sql, params }));
    },
    async prepare(sql) {
      const metadata = await rpc({ op: "prepare", sql }),
        statementId = metadata.statementId;
      return {
        sql,
        columnCount: metadata.columns.length,
        columnNames: () => metadata.columns,
        free() {
          void rpc({ op: "free", statementId }).catch(() => {});
        },
        async execute() {
          return (await rpc({ op: "execute", statementId })).changes;
        },
        async executeWithParams(params) {
          return (await rpc({ op: "execute", statementId, params })).changes;
        },
        async query() {
          return result(await rpc({ op: "query", statementId }));
        },
        async queryWithParams(params) {
          return result(await rpc({ op: "query", statementId, params }));
        },
      };
    },
    async export() {
      throw new Error("Snapshot export not part of this binding oracle");
    },
  };
  const host = new WorkerConnectionHost({
    async load() {
      return {
        FrankenDB: {
          async create() {
            return core;
          },
        },
      };
    },
  });
  const messages = new Set(),
    errors = new Set();
  const worker = {
    addEventListener(type, fn) {
      (type === "message" ? messages : errors).add(fn);
    },
    removeEventListener(type, fn) {
      (type === "message" ? messages : errors).delete(fn);
    },
    postMessage(request) {
      void host.handle(structuredClone(request)).then(
        (response) => {
          for (const fn of messages) fn({ data: structuredClone(response) });
        },
        (error) => {
          for (const fn of errors) fn({ message: String(error) });
        },
      );
    },
    terminate() {},
  };
  return {
    core,
    host,
    worker,
    rpc,
    requests,
    async shutdown() {
      process.stdin.end();
      await exit;
    },
  };
}
