// Production host + FQR1 transport. Core fixtures are explicitly not WASM.

import assert from "node:assert/strict";
import { test } from "node:test";
import { WorkerConnectionHost } from "../src/connection.ts";
import {
  BINARY_RESULT_THRESHOLD,
  decodeQueryResult,
  responseTransferList,
  tryEncodeQueryResult,
} from "../src/result-codec.ts";
import { sqliteSnapshotWorker } from "./helpers/snapshot-sqlite-core.mjs";

function result(values = [[1, "one"]], columns = ["id", "name"]) {
  return {
    columns,
    columnCount: columns.length,
    columnTypes: [],
    changes: 0,
    rowArrays: values,
    rows: values.map((row) => Object.fromEntries(columns.map((key, i) => [key, row[i]]))),
  };
}
function fixture(data = result()) {
  let queries = 0,
    loads = 0,
    creates = 0;
  const core = {
    path: ":memory:",
    free() {},
    close() {},
    async executeBatch() {},
    async query() {
      queries++;
      return data;
    },
    async queryWithParams() {
      queries++;
      return data;
    },
    async prepare(sql) {
      return {
        sql,
        columnCount: data.columnCount,
        columnNames: () => data.columns,
        free() {},
        query: () => core.query(),
        queryWithParams: () => core.query(),
      };
    },
  };
  const host = new WorkerConnectionHost({
    async load() {
      loads++;
      return {
        FrankenDB: {
          async create() {
            creates++;
            return core;
          },
          async import() {
            creates++;
            return core;
          },
        },
      };
    },
  });
  let id = 1;
  const send = (input) => host.handle({ requestId: id++, ...input });
  const init = (resultEncoding) =>
    send({ kind: "init", config: resultEncoding === undefined ? {} : { resultEncoding } });
  return { host, send, init, counts: () => ({ queries, loads, creates }) };
}

function received(response) {
  const transfer = responseTransferList(response);
  const copy = structuredClone(response, { transfer });
  return copy.kind === "query-binary-result" ? decodeQueryResult(copy.data) : copy.data;
}

test("default encoding leaves structured results untouched and never walks result rows", async () => {
  const data = result();
  let visits = 0;
  Object.defineProperty(data, "rows", {
    enumerable: true,
    get() {
      visits++;
      return [{ id: 1, name: "one" }];
    },
  });
  const f = fixture(data);
  assert.equal((await f.init()).data.resultEncoding, "structured-clone");
  const reply = await f.send({ kind: "query", sql: "SELECT 1" });
  assert.equal(reply.kind, "query-result");
  assert.equal(reply.data, data);
  assert.equal(visits, 0);
  assert.equal(f.host.requestQueue.pendingRequests, 0);
});

test("binary negotiation produces one fresh transferable buffer for direct and prepared reads", async () => {
  const bytes = new Uint8Array(1024);
  bytes.set([1, 2, 3], 33);
  const data = result([[1, bytes.subarray(33, 36)]], ["id", "blob"]);
  const f = fixture(data);
  assert.equal((await f.init("binary")).data.resultEncoding, "binary");
  const statement = await f.send({ kind: "prepare", sql: "SELECT 1" });
  for (const request of [
    { kind: "query", sql: "SELECT 1" },
    { kind: "query", sql: "SELECT ?", params: [1] },
    { kind: "statement-query", statementId: statement.data.statementId },
    { kind: "statement-query", statementId: statement.data.statementId, params: [1] },
  ]) {
    const reply = await f.send(request);
    assert.equal(reply.kind, "query-binary-result");
    assert.equal(reply.encoding, "fqr1");
    assert.deepEqual(responseTransferList(reply), [reply.data]);
    assert.deepEqual(received(reply), data);
    assert.equal(reply.data.byteLength, 0, "sender result frame must detach");
    assert.equal(bytes.byteLength, 1024, "core backing allocation must stay alive");
    assert.deepEqual([...data.rowArrays[0][1]], [1, 2, 3]);
  }
  assert.equal(f.counts().queries, 4);
  assert.equal(f.host.requestQueue.pendingRequests, 0);
});

test("auto threshold is measured on the encoded image, including the exact boundary", async () => {
  const empty = result([[new Uint8Array()]], ["b"]);
  const overhead = tryEncodeQueryResult(empty).byteLength;
  for (const delta of [-1, 0, 1]) {
    const f = fixture(
      result([[new Uint8Array(BINARY_RESULT_THRESHOLD - overhead + delta)]], ["b"]),
    );
    assert.equal((await f.init("auto")).data.resultEncoding, "auto");
    const reply = await f.send({ kind: "query", sql: "SELECT b" });
    assert.equal(reply.kind, delta < 0 ? "query-result" : "query-binary-result");
    if (delta >= 0) assert.equal(reply.data.byteLength, BINARY_RESULT_THRESHOLD + delta);
  }
});

test("reinitialization resets the encoding and invalid negotiation cannot replace a live database", async () => {
  const f = fixture();
  await f.init("binary");
  const before = f.counts();
  const bad = await f.init("typo");
  assert.equal(bad.error.code, "ERR_FSQLITE_RESULT_ENCODING");
  assert.deepEqual(f.counts(), before);
  assert.equal((await f.send({ kind: "query", sql: "SELECT 1" })).kind, "query-binary-result");
  await f.init();
  assert.equal((await f.send({ kind: "query", sql: "SELECT 1" })).kind, "query-result");
});

test("binary mode preserves scalar types, aliases, UTF-16 and empty result metadata", async () => {
  const values = [
    null,
    false,
    true,
    -0,
    NaN,
    Infinity,
    -Infinity,
    1.25,
    -(1n << 63n),
    (1n << 63n) - 1n,
    "a\0\ud800\udfff",
    new Uint8Array([0, 255]),
    11,
    22,
  ];
  const names = values.map((_, i) => "c" + i);
  names[11] = "__proto__";
  names[12] = "duplicate";
  names[13] = "duplicate";
  for (const data of [result([values], names), result([], ["empty"]), result([[]], [])]) {
    const f = fixture(data);
    await f.init("binary");
    const reply = await f.send({ kind: "query", sql: "fixture" });
    assert.equal(reply.kind, "query-binary-result");
    const actual = received(reply);
    assert.deepEqual(actual, data);
    if (actual.rows[0]?.__proto__ instanceof Uint8Array)
      assert.equal(Object.getPrototypeOf(actual.rows[0]), Object.prototype);
  }
});

test("unsupported and extended results fall back without rerunning successful SQL", async () => {
  const data = [
    result([[1n << 80n]], ["x"]),
    { ...result(), extra: { keep: true } },
    { ...result(), rows: [{ id: 1, name: "different" }] },
    result([[undefined]], ["x"]),
  ];
  for (const value of data) {
    const f = fixture(value);
    await f.init("binary");
    const reply = await f.send({ kind: "query", sql: "INSERT ... RETURNING" });
    assert.equal(reply.kind, "query-result");
    assert.equal(reply.data, value);
    assert.equal(f.counts().queries, 1);
    assert.deepEqual(responseTransferList(reply), []);
  }
});

test("result mode is captured on admission, while independent hosts remain independent", async () => {
  const a = fixture(),
    b = fixture();
  const config = { resultEncoding: "binary" };
  const begin = a.send({ kind: "init", config });
  config.resultEncoding = "structured-clone";
  await begin;
  await b.init();
  assert.equal((await a.send({ kind: "query", sql: "SELECT 1" })).kind, "query-binary-result");
  assert.equal((await b.send({ kind: "query", sql: "SELECT 1" })).kind, "query-result");
});

test("real SQL results transfer inside a managed scope without changing its rollback", async () => {
  const f = sqliteSnapshotWorker();
  let id = 1;
  const send = async (request) => {
    const reply = await f.host.handle({ requestId: id++, ...request });
    assert.notEqual(reply.kind, "error", JSON.stringify(reply));
    return reply;
  };
  await send({ kind: "init", config: { resultEncoding: "binary" } });
  await send({ kind: "execute-batch", sql: "CREATE TABLE t(id PRIMARY KEY, b BLOB)" });
  await send({ kind: "transaction", action: "begin", transactionId: "1" });
  const write = await send({
    kind: "query",
    transactionId: "1",
    sql: "INSERT INTO t VALUES(1,x'010203') RETURNING id,b",
  });
  assert.equal(write.kind, "query-binary-result");
  assert.deepEqual(received(write).rowArrays, [[1, new Uint8Array([1, 2, 3])]]);
  await send({ kind: "transaction", action: "rollback", transactionId: "1" });
  const read = await send({ kind: "query", sql: "SELECT * FROM t" });
  assert.deepEqual(received(read).rowArrays, []);
  await send({ kind: "close" });
});

test("codec refusal after real INSERT RETURNING is a successful structured reply, not a retry", async () => {
  const f = sqliteSnapshotWorker();
  await f.host.handle({ kind: "init", requestId: 1, config: { resultEncoding: "binary" } });
  await f.host.handle({
    kind: "execute-batch",
    requestId: 2,
    sql: "CREATE TABLE t(id INTEGER PRIMARY KEY)",
  });
  const core = f.handles[0],
    query = core.query.bind(core);
  core.query = async (sql) => ({ ...(await query(sql)), extension: "retained" });
  const reply = await f.host.handle({
    kind: "query",
    requestId: 3,
    sql: "INSERT INTO t DEFAULT VALUES RETURNING id",
  });
  assert.equal(reply.kind, "query-result");
  assert.equal(reply.data.extension, "retained");
  assert.equal((await query("SELECT count(*) n FROM t")).rows[0].n, 1);
  await f.host.handle({ kind: "close", requestId: 4 });
});
