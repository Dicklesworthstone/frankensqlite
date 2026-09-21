// Test-only Web Worker shim running the ACTUAL worker.ts entry point in a Node
// worker thread. Replace ONLY the unavailable WASM core with Node SQLite.

import { mkdtemp, readFile, writeFile } from "node:fs/promises";
import { register } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { parentPort, workerData } from "node:worker_threads";

const coreSource =
  "export default async function() {};" +
  "export const FrankenDB = globalThis.__resultReferenceCore;";
register(
  "data:text/javascript," +
    encodeURIComponent(`
  export async function resolve(specifier, context, next) {
    if (specifier === '@frankensqlite/core') return {
      url: ${JSON.stringify("data:text/javascript," + encodeURIComponent(coreSource))}, shortCircuit: true };
    return next(specifier, context);
  }
`),
  import.meta.url,
);

async function open(bytes) {
  const directory = await mkdtemp(join(tmpdir(), "fsqlite-result-reference-"));
  const path = workerData?.path ?? join(directory, "db.sqlite");
  if (bytes !== undefined) await writeFile(path, bytes);
  const db = new DatabaseSync(path);
  let closed = false,
    exports = 0;
  const parameters = (values) => values.map((v) => (typeof v === "boolean" ? Number(v) : v));
  function prepared(sql) {
    const stmt = db.prepare(sql);
    stmt.setReadBigInts(true);
    const columns = stmt.columns().map((c) => c.name);
    if (new Set(columns).size !== columns.length) {
      // Node 22 lacks positional result arrays: do not falsely certify duplicate
      // aliases through an object-only oracle. Typed codec fixtures cover them.
      throw new Error("Duplicate aliases require the typed result fixture");
    }
    let freed = false;
    function live() {
      if (freed || closed) throw new Error("Reference handle is closed");
    }
    const read = async (values) => {
      live();
      const rows = stmt.all(...parameters(values)).map((row) => ({ ...row }));
      const result = {
        columns,
        columnCount: columns.length,
        columnTypes: [],
        rows,
        rowArrays: rows.map((row) => columns.map((name) => row[name])),
        changes: 0,
      };
      if (workerData?.extraMetadata) result.extension = { keep: true };
      return result;
    };
    return {
      sql,
      columnCount: columns.length,
      columnNames: () => columns,
      free() {
        live();
        freed = true;
      },
      async execute() {
        live();
        return Number(stmt.run().changes);
      },
      async executeWithParams(values) {
        live();
        return Number(stmt.run(...parameters(values)).changes);
      },
      query: () => read([]),
      queryWithParams: read,
    };
  }
  return {
    path,
    close() {
      if (!closed) {
        db.close();
        closed = true;
      }
    },
    free() {},
    async execute(sql) {
      return prepared(sql).execute();
    },
    async executeWithParams(sql, params) {
      return prepared(sql).executeWithParams(params);
    },
    async executeBatch(sql) {
      db.exec(sql);
    },
    async query(sql) {
      return prepared(sql).query();
    },
    async queryWithParams(sql, params) {
      return prepared(sql).queryWithParams(params);
    },
    async prepare(sql) {
      return prepared(sql);
    },
    async export() {
      const target = join(directory, `export-${++exports}.sqlite`);
      db.prepare("VACUUM INTO ?").run(target);
      return new Uint8Array(await readFile(target));
    },
  };
}
globalThis.__resultReferenceCore = { create: () => open(), import: (bytes) => open(bytes) };
globalThis.addEventListener = (type, listener) => {
  // Test controls use a separate logical channel, never the production wire.
  if (type === "message")
    parentPort.on("message", (data) => {
      if (data?.fixture === undefined) listener({ data });
    });
  if (type === "messageerror") parentPort.on("messageerror", () => listener({ data: undefined }));
};
globalThis.postMessage = (response, transfer = []) => {
  const before = transfer.map((buffer) => buffer.byteLength);
  parentPort.postMessage(response, transfer);
  parentPort.postMessage({
    audit: true,
    id: response.requestId,
    kind: response.kind,
    before,
    after: transfer.map((buffer) => buffer.byteLength),
  });
};
await import("../../src/worker.ts");
parentPort.postMessage({ booted: true });
