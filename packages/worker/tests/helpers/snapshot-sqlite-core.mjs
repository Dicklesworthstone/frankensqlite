// Test-only SQLite reference adapter. This is NOT FrankenSQLite WASM.
// Uses real SQLite files for export/import and real SQL for transaction checks.

import { mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { parameterLayout } from "../../src/bindings.ts";
import { WorkerConnectionHost } from "../../src/connection.ts";

export function sqliteSnapshotWorker(hooks = {}) {
  const events = [];
  const handles = [];
  let creates = 0;
  let imports = 0;
  async function open(bytes) {
    const directory = await mkdtemp(join(tmpdir(), "fsqlite-snapshot-reference-"));
    const path = join(directory, "database.sqlite");
    if (bytes) await writeFile(path, bytes);
    const db = new DatabaseSync(path);
    let closed = false;
    let exports = 0;
    function parameters(statement, params) {
      if (params.length === 0) return [];
      const { names } = parameterLayout(statement.sourceSQL);
      const named = Object.create(null);
      const positional = [];
      for (const [index, item] of params.entries()) {
        const value = typeof item === "boolean" ? Number(item) : item;
        const name = names[index];
        // node:sqlite accepts ?, ?NNN and unused numeric slots positionally.
        // Its :name, @name and $name parameters require an object argument.
        if (name && !name.startsWith("?")) named[name] = value;
        else positional.push(value);
      }
      if (!Object.keys(named).length) return positional;
      statement.setAllowBareNamedParameters(false);
      return [named, ...positional];
    }
    const query = (sql, params = []) => {
      const statement = db.prepare(sql);
      const columns = statement.columns().map((c) => c.name);
      const rows = statement.all(...parameters(statement, params));
      return {
        columns,
        columnCount: columns.length,
        columnTypes: [],
        rows,
        rowArrays: rows.map((row) => columns.map((c) => row[c])),
        changes: 0,
      };
    };
    const core = {
      path,
      close() {
        events.push("close");
        hooks.beforeClose?.();
        if (!closed) {
          db.close();
          closed = true;
        }
      },
      free() {
        events.push("free");
        if (!closed) {
          db.close();
          closed = true;
        }
      },
      async execute(sql) {
        events.push(sql);
        await hooks.beforeExecute?.(sql, [], db);
        return Number(db.prepare(sql).run().changes);
      },
      async executeWithParams(sql, params) {
        events.push(sql);
        await hooks.beforeExecute?.(sql, params, db);
        const statement = db.prepare(sql);
        return Number(statement.run(...parameters(statement, params)).changes);
      },
      async executeBatch(sql) {
        events.push(sql);
        await hooks.beforeBatch?.(sql);
        db.exec(sql);
      },
      async query(sql) {
        return query(sql);
      },
      async queryWithParams(sql, params) {
        return query(sql, params);
      },
      async prepare(sql) {
        const statement = db.prepare(sql);
        const columns = statement.columns().map((c) => c.name);
        let freed = false;
        return {
          sql,
          columnCount: columns.length,
          columnNames: () => columns,
          free() {
            if (freed) throw new Error("double finalize");
            freed = true;
            hooks.statementFree?.(sql);
          },
          async execute() {
            if (freed) throw new Error("freed");
            await hooks.beforeExecute?.(sql, [], db);
            return Number(statement.run().changes);
          },
          async executeWithParams(params) {
            if (freed) throw new Error("freed");
            await hooks.beforeExecute?.(sql, params, db);
            return Number(statement.run(...parameters(statement, params)).changes);
          },
          async query() {
            return query(sql);
          },
          async queryWithParams(params) {
            return query(sql, params);
          },
        };
      },
      async export() {
        events.push("export");
        await hooks.beforeExport?.();
        const target = join(directory, `snapshot-${++exports}.sqlite`);
        db.prepare("VACUUM INTO ?").run(target);
        return new Uint8Array(await readFile(target));
      },
    };
    handles.push(core);
    return core;
  }
  const host = new WorkerConnectionHost(
    {
      async load() {
        return {
          FrankenDB: {
            async create(path) {
              if (path !== ":memory:") throw new Error("The WASM core requires an in-memory path");
              creates++;
              return open();
            },
            async import(bytes) {
              imports++;
              await hooks.beforeImport?.();
              return open(bytes);
            },
          },
        };
      },
    },
    hooks.requestLimits,
  );
  const messages = new Set();
  const errors = new Set();
  let terminated = false;
  const worker = {
    requests: [],
    terminateCount: 0,
    addEventListener(type, listener) {
      (type === "message" ? messages : errors).add(listener);
    },
    removeEventListener(type, listener) {
      (type === "message" ? messages : errors).delete(listener);
    },
    postMessage(request, transfer) {
      if (terminated) throw new Error("worker is terminated");
      this.requests.push(structuredClone(request));
      const copy = structuredClone(request, transfer ? { transfer } : undefined);
      void host.handle(copy).then(
        (response) => {
          if (!terminated)
            for (const listener of messages) listener({ data: structuredClone(response) });
        },
        (error) => {
          for (const listener of errors) listener({ message: String(error) });
        },
      );
    },
    terminate() {
      this.terminateCount++;
      terminated = true;
    },
  };
  return { host, worker, events, handles, counts: () => ({ creates, imports }) };
}
