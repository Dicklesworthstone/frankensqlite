// Production WorkerConnectionHost with a file-backed SQLite reference core.
// This covers ownership, staging and real import/export; not FrankenSQLite WASM.
import assert from "node:assert/strict";
import { mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import test from "node:test";
import { setImmediate as turn } from "node:timers/promises";
import { WorkerConnectionHost } from "../src/connection.ts";
import { installIndexedDbModel } from "./helpers/indexeddb-model.mjs";

const fatal = "ERR_FSQLITE_TRANSACTION_CONNECTION_UNUSABLE";
function deferred() {
  let resolve;
  const promise = new Promise((r) => {
    resolve = r;
  });
  return { promise, resolve };
}
function ok(response, kind) {
  assert.notEqual(response.kind, "error", JSON.stringify(response));
  if (kind) assert.equal(response.kind, kind);
  return response;
}
function error(response, code) {
  assert.equal(response.kind, "error", JSON.stringify(response));
  if (code) assert.equal(response.error.code, code);
  return response.error;
}
async function fixture(t, hooks = {}, config = {}) {
  const records = [],
    events = [];
  const storage = installIndexedDbModel();
  const directory = await mkdtemp(join(tmpdir(), "fsqlite-staged-init-"));
  let created = 0,
    loaded = 0,
    exported = 0,
    requestId = 0;
  const open = async (bytes) => {
    const generation = ++created;
    await hooks.beforeOpen?.(generation, bytes);
    if (hooks.alias) return hooks.alias;
    const path = join(directory, `database-${generation}.sqlite`);
    if (bytes !== undefined) await writeFile(path, bytes);
    const raw = new DatabaseSync(path);
    // The reference import must really parse the stored schema, not simply
    // return a handle for corrupt bytes whose first later query would fail.
    try {
      raw.prepare("SELECT name FROM sqlite_schema").all();
    } catch (cause) {
      raw.close();
      throw cause;
    }
    const record = { generation, raw, closed: false, closes: 0, frees: 0, statementFrees: 0 };
    const query = (sql, params = []) => {
      const statement = raw.prepare(sql),
        columns = statement.columns().map((c) => c.name);
      const rows = statement.all(...params);
      return {
        columns,
        columnCount: columns.length,
        columnTypes: [],
        rows,
        rowArrays: rows.map((row) => columns.map((c) => row[c])),
        changes: 0,
      };
    };
    const handle = {
      get path() {
        return hooks.path ? hooks.path(generation, path) : path;
      },
      close() {
        record.closes++;
        events.push(`close:${generation}`);
        hooks.close?.(generation);
        if (!record.closed) {
          raw.close();
          record.closed = true;
        }
      },
      free() {
        record.frees++;
        events.push(`free:${generation}`);
        hooks.free?.(generation);
        if (!record.closed) {
          raw.close();
          record.closed = true;
        }
      },
      async execute(sql) {
        return Number(raw.prepare(sql).run().changes);
      },
      async executeWithParams(sql, params) {
        return Number(raw.prepare(sql).run(...params).changes);
      },
      async executeBatch(sql) {
        raw.exec(sql);
      },
      async query(sql) {
        return query(sql);
      },
      async queryWithParams(sql, params) {
        return query(sql, params);
      },
      async prepare(sql) {
        const statement = raw.prepare(sql),
          columns = statement.columns().map((c) => c.name);
        let freed = false;
        const check = () => {
          if (freed) throw new Error("statement freed");
        };
        return {
          sql,
          columnCount: columns.length,
          columnNames: () => columns,
          free() {
            assert.equal(freed, false);
            freed = true;
            record.statementFrees++;
            hooks.statementFree?.(generation);
          },
          async execute() {
            check();
            return Number(statement.run().changes);
          },
          async executeWithParams(params) {
            check();
            return Number(statement.run(...params).changes);
          },
          async query() {
            check();
            return query(sql);
          },
          async queryWithParams(params) {
            check();
            return query(sql, params);
          },
        };
      },
      async export() {
        const target = join(directory, `export-${++exported}.sqlite`);
        raw.prepare("VACUUM INTO ?").run(target);
        return new Uint8Array(await readFile(target));
      },
    };
    // Keep counters and close state shared with the handle methods.
    records.push(Object.assign(record, { handle }));
    return handle;
  };
  const host = new WorkerConnectionHost({
    async load() {
      loaded++;
      await hooks.load?.(loaded);
      return { FrankenDB: { create: () => open(), import: (bytes) => open(bytes) } };
    },
  });
  const send = (request) => host.handle(structuredClone({ ...request, requestId: ++requestId }));
  t.after(async () => {
    await send({ kind: "close" });
    // Fault injection may deliberately make both candidate destructors throw.
    // This test-only cleanup does not count as successful production cleanup.
    for (const record of records)
      if (!record.closed) {
        record.raw.close();
        record.closed = true;
      }
  });
  const initialize = (next) => send({ kind: "init", config: next });
  ok(await initialize(config));
  ok(
    await send({
      kind: "execute-batch",
      sql: "CREATE TABLE items(v); INSERT INTO items VALUES(7);",
    }),
  );
  const statementId = ok(await send({ kind: "prepare", sql: "SELECT v FROM items" })).data
    .statementId;
  const query = async () =>
    ok(await send({ kind: "query", sql: "SELECT v FROM items" })).data.rowArrays;
  return { host, send, initialize, query, records, events, storage, statementId, hooks, directory };
}
function malformedImage() {
  const bytes = new Uint8Array(512);
  bytes.set(new TextEncoder().encode("SQLite format 3\0"));
  bytes[16] = 2;
  return bytes; // Page-aligned envelope, but no valid database header/schema.
}

for (const mode of ["import", "create", "loader"]) {
  test(`failed ${mode} retains unsaved data and the existing prepared handle`, async (t) => {
    const f = await fixture(t);
    ok(await f.send({ kind: "execute", sql: "UPDATE items SET v=9" }));
    if (mode === "create")
      f.hooks.beforeOpen = () => {
        throw new Error("create failed");
      };
    if (mode === "loader")
      f.hooks.load = () => {
        throw new Error("loader failed");
      };
    const result = await f.initialize(mode === "import" ? { snapshot: malformedImage() } : {});
    error(result);
    assert.deepEqual(await f.query(), [[9]]);
    assert.deepEqual(
      ok(await f.send({ kind: "statement-query", statementId: f.statementId })).data.rowArrays,
      [[9]],
    );
    assert.equal(f.records[0].closes, 0);
    assert.equal(f.records[0].statementFrees, 0);
    const bytes = ok(await f.send({ kind: "export" })).data;
    const path = join(f.directory, "retained.sqlite");
    await writeFile(path, bytes);
    const reopened = new DatabaseSync(path);
    try {
      assert.equal(reopened.prepare("SELECT v FROM items").get().v, 9);
      assert.equal(reopened.prepare("PRAGMA integrity_check").get().integrity_check, "ok");
    } finally {
      reopened.close();
    }
  });
}

test("failed replacement preserves a manually owned transaction and its rollback boundary", async (t) => {
  const f = await fixture(t);
  ok(await f.send({ kind: "execute-batch", sql: "BEGIN; UPDATE items SET v=12;" }));
  error(await f.initialize({ snapshot: malformedImage() }));
  assert.deepEqual(await f.query(), [[12]]);
  ok(await f.send({ kind: "execute-batch", sql: "ROLLBACK;" }));
  assert.deepEqual(await f.query(), [[7]]);
});

test("queued requests wait for staged failure, then continue against the retained database", async (t) => {
  const entered = deferred(),
    release = deferred();
  const f = await fixture(t);
  f.hooks.beforeOpen = async () => {
    entered.resolve();
    await release.promise;
    throw new Error("late import failure");
  };
  const init = f.initialize({ snapshot: malformedImage() });
  await entered.promise;
  const query = f.query();
  let settled = false;
  void query.then(() => {
    settled = true;
  });
  await turn();
  assert.equal(settled, false);
  assert.equal(f.host.requestQueue.pendingRequests, 2);
  assert.equal(f.records[0].closes, 0);
  release.resolve();
  assert.match(error(await init).message, /late import failure/);
  assert.deepEqual(await query, [[7]]);
  assert.equal(f.host.requestQueue.pendingRequests, 0);
});

test("successful replacement publishes once and invalidates old prepared handles without id reuse", async (t) => {
  const f = await fixture(t);
  const image = ok(await f.send({ kind: "export" })).data;
  ok(await f.send({ kind: "execute", sql: "UPDATE items SET v=99" }));
  ok(await f.initialize({ snapshot: image }), "ready");
  assert.deepEqual(await f.query(), [[7]]);
  assert.deepEqual(
    [f.records[0].closes, f.records[0].frees, f.records[0].statementFrees],
    [1, 1, 1],
  );
  assert.equal(f.records[1].closes, 0);
  error(await f.send({ kind: "statement-query", statementId: f.statementId }));
  const newId = ok(await f.send({ kind: "prepare", sql: "SELECT v FROM items" })).data.statementId;
  assert.notEqual(newId, f.statementId);
  assert.deepEqual(
    ok(await f.send({ kind: "statement-query", statementId: newId })).data.rowArrays,
    [[7]],
  );
});

test("a loader cannot return the live handle as a candidate and cause self-destruction", async (t) => {
  const f = await fixture(t);
  f.hooks.alias = f.records[0].handle;
  assert.match(error(await f.initialize({})).message, /separately owned/);
  assert.deepEqual(await f.query(), [[7]]);
  assert.deepEqual(
    [f.records[0].closes, f.records[0].frees, f.records[0].statementFrees],
    [0, 0, 0],
  );
});

for (const invalid of ["throwing", "wrong-type"]) {
  test(`candidate ${invalid} metadata is rejected before disposing the previous session`, async (t) => {
    const f = await fixture(t);
    f.hooks.path = (generation, path) => {
      if (generation === 1) return path;
      if (invalid === "throwing") throw new Error("path metadata failure");
      return {};
    };
    error(await f.initialize({}));
    assert.deepEqual(await f.query(), [[7]]);
    assert.deepEqual([f.records[1].closes, f.records[1].frees], [1, 1]);
    assert.equal(f.records[0].closes, 0);
  });
}

test("candidate destructor failures retain every cause without closing the healthy previous session", async (t) => {
  const f = await fixture(t);
  f.hooks.path = (generation) => {
    if (generation === 2) throw new Error("candidate path");
    return "old";
  };
  f.hooks.close = (generation) => {
    if (generation === 2) throw new Error("candidate close");
  };
  f.hooks.free = (generation) => {
    if (generation === 2) throw new Error("candidate free");
  };
  const failure = error(await f.initialize({}), "ERR_FSQLITE_WORKER_INITIALIZATION");
  assert.equal(failure.cause.message, "candidate path");
  assert.deepEqual(
    failure.cleanupErrors.map((e) => e.message),
    ["candidate close", "candidate free"],
  );
  assert.deepEqual(await f.query(), [[7]]);
  assert.equal(f.records[0].closes, 0);
  assert.deepEqual([f.records[1].closes, f.records[1].frees], [1, 1]);
});

test("old-session teardown failure never publishes the candidate and fences queued SQL", async (t) => {
  const f = await fixture(t);
  f.hooks.statementFree = (generation) => {
    if (generation === 1) throw new Error("old statement");
  };
  f.hooks.close = (generation) => {
    if (generation === 1) throw new Error("old close");
  };
  f.hooks.free = (generation) => {
    if (generation === 1) throw new Error("old free");
  };
  const init = f.initialize({});
  const tail = f.send({ kind: "execute", sql: "CREATE TABLE should_not_exist(v)" });
  const failure = error(await init, fatal);
  assert.equal(failure.cause.cause.message, "old statement");
  assert.deepEqual(
    failure.cause.cleanupErrors.map((e) => e.message),
    ["old close", "old free"],
  );
  assert.equal(failure.userRecoverable, false);
  error(await tail, fatal);
  assert.deepEqual(
    [f.records[0].closes, f.records[0].frees, f.records[0].statementFrees],
    [1, 1, 1],
  );
  assert.deepEqual([f.records[1].closes, f.records[1].frees], [1, 1]);
});

for (const value of [undefined, null, false, 0, ""]) {
  test(`a falsy thrown destructor value (${String(value)}) cannot become successful initialization`, async (t) => {
    const f = await fixture(t);
    f.hooks.statementFree = (generation) => {
      if (generation === 1) throw value;
    };
    error(await f.initialize({}), fatal);
    error(await f.send({ kind: "execute", sql: "CREATE TABLE ghost(v)" }), fatal);
    assert.equal(f.records[1].closed, true);
  });
}

test("transport failure during staging joins candidate creation and closes both generations", async (t) => {
  const f = await fixture(t),
    entered = deferred(),
    release = deferred();
  f.hooks.beforeOpen = async () => {
    entered.resolve();
    await release.promise;
  };
  const init = f.initialize({});
  await entered.promise;
  const close = f.host.failTransport(new Error("transport disappeared"));
  await turn();
  assert.equal(f.records[0].closes, 0);
  release.resolve();
  assert.match(error(await init).message, /transport disappeared/);
  ok(await close);
  assert.deepEqual(
    f.records.map((r) => [r.closes, r.frees]),
    [
      [1, 1],
      [1, 1],
    ],
  );
});

test("ordinary close queued after initialization drains the successful replacement in FIFO order", async (t) => {
  const f = await fixture(t),
    entered = deferred(),
    release = deferred();
  f.hooks.beforeOpen = async () => {
    entered.resolve();
    await release.promise;
  };
  const init = f.initialize({});
  await entered.promise;
  const close = f.send({ kind: "close" });
  await turn();
  assert.equal(f.records[0].closes, 0);
  release.resolve();
  ok(await init, "ready");
  ok(await close, "close-result");
  assert.deepEqual(
    f.records.map((r) => [r.closes, r.frees]),
    [
      [1, 1],
      [1, 1],
    ],
  );
});

test("a reentrant transport fence during retirement cannot publish the candidate", async (t) => {
  const f = await fixture(t);
  let close;
  f.hooks.close = (generation) => {
    if (generation === 1) close = f.host.failTransport(new Error("retirement transport failure"));
  };
  const failure = error(await f.initialize({}), fatal);
  assert.equal(failure.cause.message, "retirement transport failure");
  ok(await close, "close-result");
  assert.deepEqual(
    f.records.map((r) => [r.closes, r.frees]),
    [
      [1, 1],
      [1, 1],
    ],
  );
});

test("failed snapshot replacement preserves the original store and checkpoint parent revision", async (t) => {
  const f = await fixture(t, {}, { dbName: "original", persistence: "indexeddb-snapshot" });
  const first = ok(await f.send({ kind: "checkpoint" })).data;
  ok(await f.send({ kind: "execute", sql: "UPDATE items SET v=11" }));
  error(
    await f.initialize({
      dbName: "replacement",
      persistence: "indexeddb-snapshot",
      snapshot: malformedImage(),
    }),
  );
  assert.deepEqual(await f.query(), [[11]]);
  const second = ok(await f.send({ kind: "checkpoint" })).data;
  assert.equal(second.parentRevision, first.revision);
  const states = f.storage.databases;
  assert.equal(states.get("frankensqlite:snapshot:v1:original").connections.size, 1);
  assert.equal(states.get("frankensqlite:snapshot:v1:replacement").connections.size, 0);
  assert.equal(states.get("frankensqlite:snapshot:v1:replacement").values.size, 0);
});

test("managed transaction ownership rejects replacement before touching either generation", async (t) => {
  const f = await fixture(t);
  ok(await f.send({ kind: "transaction", action: "begin", transactionId: "1" }));
  error(await f.initialize({ snapshot: malformedImage() }), "ERR_FSQLITE_TRANSACTION_OWNERSHIP");
  ok(await f.send({ kind: "execute", transactionId: "1", sql: "UPDATE items SET v=12" }));
  ok(await f.send({ kind: "transaction", action: "commit", transactionId: "1" }));
  assert.deepEqual(await f.query(), [[12]]);
  assert.equal(f.records.length, 1);
});
