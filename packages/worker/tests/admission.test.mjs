// Direct production host admission; real SQLite reference, no SDK-side gate.

import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import { sqliteSnapshotWorker } from "./helpers/snapshot-sqlite-core.mjs";

function deferred() {
  let resolve;
  const promise = new Promise((yes) => {
    resolve = yes;
  });
  return { promise, resolve };
}
const options = { timeout: 5000 };
async function fixture(limits = {}) {
  const f = sqliteSnapshotWorker({ requestLimits: limits });
  const ready = await f.host.handle({ kind: "init", requestId: 1, config: {} });
  assert.equal(ready.kind, "ready");
  await f.handles[0].execute("CREATE TABLE t(id INTEGER PRIMARY KEY, value TEXT)");
  const query = f.handles[0].query.bind(f.handles[0]);
  return {
    ...f,
    read: async () => (await query("SELECT * FROM t ORDER BY id")).rowArrays,
    close: () => f.host.handle({ kind: "close", requestId: 99999 }),
  };
}
function blockQuery(f) {
  const entered = deferred(),
    release = deferred();
  const original = f.handles[0].query.bind(f.handles[0]);
  f.handles[0].query = async (sql) => {
    entered.resolve();
    await release.promise;
    return original(sql);
  };
  return { entered: entered.promise, release: release.resolve };
}
function request(requestId, sql = "SELECT * FROM t") {
  return { kind: "query", requestId, sql };
}
function errorCode(response, code) {
  assert.equal(response.kind, "error");
  assert.equal(response.error.code, code);
}

test(
  "host counts active plus queued requests and refuses overflow before database work",
  options,
  async () => {
    const f = await fixture({ maxPendingRequests: 2 });
    const gate = blockQuery(f);
    const first = f.host.handle(request(2));
    await gate.entered;
    const second = f.host.handle({
      kind: "execute",
      requestId: 3,
      sql: "INSERT INTO t VALUES(1,'one')",
    });
    const refused = await f.host.handle({
      kind: "execute",
      requestId: 4,
      sql: "INSERT INTO t VALUES(2,'two')",
    });
    errorCode(refused, "ERR_FSQLITE_QUEUE_FULL");
    assert.equal(refused.error.transient, true);
    assert.equal(f.host.requestQueue.pendingRequests, 2);
    assert.deepEqual(await f.read(), []);
    gate.release();
    assert.equal((await first).kind, "query-result");
    assert.equal((await second).kind, "execute-result");
    assert.deepEqual(await f.read(), [[1, "one"]]);
    assert.equal(f.host.requestQueue.pendingBytes, 0);
    await f.close();
  },
);

test(
  "host byte ceiling includes SQL, parameters, snapshots and whole blob backing stores",
  options,
  async () => {
    const f = await fixture({ maxPendingBytes: 512 });
    for (const req of [
      { kind: "execute", requestId: 2, sql: "x".repeat(500) },
      {
        kind: "execute",
        requestId: 3,
        sql: "INSERT INTO t VALUES(1,?)",
        params: ["x".repeat(500)],
      },
      {
        kind: "execute",
        requestId: 4,
        sql: "INSERT INTO t VALUES(1,?)",
        params: [new Uint8Array(new ArrayBuffer(4096), 0, 1)],
      },
      { kind: "init", requestId: 5, config: { snapshot: new Uint8Array(4096) } },
    ])
      errorCode(await f.host.handle(req), "ERR_FSQLITE_REQUEST_TOO_LARGE");
    assert.deepEqual(await f.read(), []);
    assert.equal(f.counts().creates, 1);
    assert.equal(f.counts().imports, 0);
    assert.equal(f.host.requestQueue.pendingBytes, 0);
    await f.close();
  },
);

test(
  "cumulative host byte pressure recovers after the blocked predecessor settles",
  options,
  async () => {
    const f = await fixture({ maxPendingBytes: 512 });
    const gate = blockQuery(f);
    const first = f.host.handle(request(2, `SELECT * FROM t /*${"x".repeat(90)}*/`));
    await gate.entered;
    errorCode(await f.host.handle(request(3)), "ERR_FSQLITE_QUEUE_FULL");
    gate.release();
    await first;
    assert.equal((await f.host.handle(request(4))).kind, "query-result");
    assert.equal(f.host.requestQueue.pendingBytes, 0);
    await f.close();
  },
);

test("queue admission precedes creation of cancellation tokens", options, async () => {
  const f = await fixture({ maxPendingRequests: 1 });
  const gate = blockQuery(f);
  const first = f.host.handle(request(2));
  await gate.entered;
  errorCode(
    await f.host.handle({
      kind: "execute-many",
      requestId: 3,
      sql: "INSERT INTO t VALUES(?,?)",
      parameterSets: [[1, "bad"]],
      cancellable: true,
    }),
    "ERR_FSQLITE_QUEUE_FULL",
  );
  const cancel = await f.host.handle({ kind: "cancel-bulk", requestId: 4, targetRequestId: 3 });
  assert.equal(cancel.accepted, false);
  gate.release();
  await first;
  assert.deepEqual(await f.read(), []);
  await f.close();
});

test(
  "close and cancellation stay available under saturation and preserve FIFO cleanup",
  options,
  async () => {
    const f = await fixture({ maxPendingRequests: 1 });
    const entered = deferred(),
      release = deferred();
    const prepare = f.handles[0].prepare.bind(f.handles[0]);
    f.handles[0].prepare = async (sql) => {
      const stmt = await prepare(sql),
        run = stmt.executeWithParams.bind(stmt);
      stmt.executeWithParams = async (params) => {
        if (params[0] === 2) {
          entered.resolve();
          await release.promise;
        }
        return run(params);
      };
      return stmt;
    };
    const batch = f.host.handle({
      kind: "execute-many",
      requestId: 2,
      sql: "INSERT INTO t VALUES(?,?)",
      parameterSets: [
        [1, "a"],
        [2, "b"],
        [3, "c"],
      ],
      cancellable: true,
    });
    await entered.promise;
    const close = f.host.handle({ kind: "close", requestId: 3 });
    const repeatedClose = f.host.handle({ kind: "close", requestId: 4 });
    assert.equal(f.events.includes("close"), false);
    assert.equal(f.host.requestQueue.pendingRequests, 1);
    errorCode(await f.host.handle(request(5)), "ERR_FSQLITE_CONNECTION_CLOSED");
    assert.equal(
      (await f.host.handle({ kind: "cancel-bulk", requestId: 6, targetRequestId: 2 })).accepted,
      true,
    );
    release.resolve();
    errorCode(await batch, "ERR_FSQLITE_BULK_CANCELLED");
    assert.equal((await close).requestId, 3);
    assert.equal((await repeatedClose).requestId, 4);
    assert.equal(f.events.filter((e) => e === "close").length, 1);
    assert.equal(f.host.requestQueue.pendingBytes, 0);
    const oracle = new DatabaseSync(f.handles[0].path);
    try {
      assert.deepEqual(oracle.prepare("SELECT * FROM t").all(), []);
    } finally {
      oracle.close();
    }
    errorCode(
      await f.host.handle({ kind: "init", requestId: 7, config: {} }),
      "ERR_FSQLITE_CONNECTION_CLOSED",
    );
  },
);

test(
  "raw request IDs and duplicate requests cannot corrupt budget accounting",
  options,
  async () => {
    const f = await fixture({ maxPendingRequests: 3 });
    const gate = blockQuery(f);
    const held = f.host.handle(request(2));
    await gate.entered;
    errorCode(await f.host.handle(request(2)), "ERR_FSQLITE_REQUEST_INPUT");
    for (const id of [NaN, Infinity, 1.5, Number.MAX_SAFE_INTEGER + 1]) {
      errorCode(await f.host.handle(request(id)), "ERR_FSQLITE_REQUEST_INPUT");
    }
    assert.equal(f.host.requestQueue.pendingRequests, 1);
    gate.release();
    await held;
    assert.equal(f.host.requestQueue.pendingBytes, 0);
    await f.close();
  },
);

test(
  "queued request data is captured once, without unaccounted extra fields",
  options,
  async () => {
    const f = await fixture({ maxPendingRequests: 2, maxPendingBytes: 1024 });
    const gate = blockQuery(f);
    const first = f.host.handle(request(2));
    await gate.entered;
    let reads = 0;
    const params = [1, "original"];
    Object.defineProperty(params, "1", {
      get() {
        reads++;
        return "original";
      },
    });
    const queued = {
      kind: "execute",
      requestId: 3,
      sql: "INSERT INTO t VALUES(?,?)",
      params,
      hidden: new Uint8Array(4096),
    };
    const second = f.host.handle(queued);
    queued.sql = "DROP TABLE t";
    params[0] = 99;
    assert.equal(reads, 1);
    gate.release();
    await Promise.all([first, second]);
    assert.deepEqual(await f.read(), [[1, "original"]]);
    await f.close();
  },
);

test(
  "a close reentered from request getters never allows SQL after its fence",
  options,
  async () => {
    const f = await fixture();
    let close;
    const params = [0];
    Object.defineProperty(params, "0", {
      get() {
        close = f.host.handle({ kind: "close", requestId: 3 });
        return 1;
      },
    });
    errorCode(
      await f.host.handle({
        kind: "execute",
        requestId: 2,
        sql: "INSERT INTO t VALUES(?,'bad')",
        params,
      }),
      "ERR_FSQLITE_CONNECTION_CLOSED",
    );
    await close;
    assert.equal(f.host.requestQueue.pendingRequests, 0);
    const oracle = new DatabaseSync(f.handles[0].path);
    try {
      assert.deepEqual(oracle.prepare("SELECT * FROM t").all(), []);
    } finally {
      oracle.close();
    }
  },
);

test("host releases the lease even when error serialization throws", options, async () => {
  const f = await fixture({ maxPendingRequests: 2 });
  const entered = deferred(),
    release = deferred();
  const failure = new Error("getter failed");
  f.handles[0].execute = async () => {
    entered.resolve();
    await release.promise;
    throw {
      get code() {
        throw failure;
      },
    };
  };
  const failing = f.host.handle({ kind: "execute", requestId: 2, sql: "FAIL" });
  const rejected = assert.rejects(failing, (e) => e === failure);
  await entered.promise;
  const later = f.host.handle(request(3));
  release.resolve();
  await rejected;
  assert.equal((await later).kind, "query-result");
  assert.equal(f.host.requestQueue.pendingBytes, 0);
  await f.close();
});

test("host SQL failures return capacity and do not poison later requests", options, async () => {
  const f = await fixture({ maxPendingRequests: 1 });
  assert.equal(
    (await f.host.handle({ kind: "execute", requestId: 2, sql: "INSERT INTO absent VALUES(1)" }))
      .kind,
    "error",
  );
  const next = await f.host.handle({
    kind: "execute",
    requestId: 3,
    sql: "INSERT INTO t VALUES(1,'ok')",
  });
  assert.equal(next.kind, "execute-result");
  assert.deepEqual(await f.read(), [[1, "ok"]]);
  assert.equal(f.host.requestQueue.pendingBytes, 0);
  await f.close();
});

test("host input rejection retains the exact failing parameter-set index", options, async () => {
  const f = await fixture();
  const response = await f.host.handle({
    kind: "execute-many",
    requestId: 2,
    sql: "INSERT INTO t VALUES(?,?)",
    parameterSets: [[1, "a"], null],
  });
  errorCode(response, "ERR_FSQLITE_BULK_INPUT");
  assert.equal(response.error.batchIndex, 1);
  assert.deepEqual(await f.read(), []);
  await f.close();
});

test("host limits are independent for separate connections", options, async () => {
  const a = await fixture({ maxPendingRequests: 1 }),
    b = await fixture({ maxPendingRequests: 1 });
  const gate = blockQuery(a);
  const pending = a.host.handle(request(2));
  await gate.entered;
  assert.equal((await b.host.handle(request(2))).kind, "query-result");
  gate.release();
  await pending;
  await Promise.all([a.close(), b.close()]);
});
