// Public SDK + production worker; real Node SQLite, NOT FrankenSQLite WASM.

import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import { sqliteSnapshotWorker } from "../../worker/tests/helpers/snapshot-sqlite-core.mjs";
import { FrankenDB } from "../src/database.ts";

async function fixture(hooks = {}) {
  const f = sqliteSnapshotWorker(hooks);
  const db = await FrankenDB.open({ worker: f.worker });
  await db.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT UNIQUE)");
  await db.execute("INSERT INTO items VALUES(1,'seed')");
  const rows = async () => (await db.query("SELECT id,value FROM items ORDER BY id")).rowArrays;
  return { ...f, db, rows };
}
function diskRows(f) {
  const db = new DatabaseSync(f.handles[0].path);
  try {
    return db
      .prepare("SELECT id,value FROM items ORDER BY id")
      .all()
      .map((r) => [r.id, r.value]);
  } finally {
    db.close();
  }
}
const sqlError = "ERR_FSQLITE_TRANSACTION_SQL";

for (const method of ["execute", "query", "prepare"]) {
  test(`public managed ${method} cannot commit early or escape rollback`, async () => {
    const f = await fixture();
    await assert.rejects(
      f.db.transaction(async (tx) => {
        await tx.execute("INSERT INTO items VALUES(2,'rollback')");
        await tx[method](" /* hidden boundary */ END TRANSACTION");
      }),
      { code: sqlError },
    );
    assert.deepEqual(await f.rows(), [[1, "seed"]]);
    await f.db.close();
  });
}

for (const method of ["execute", "prepared"]) {
  test(`already-queued ${method} cannot autocommit after OR ROLLBACK`, async () => {
    const f = await fixture();
    let outcomes;
    await assert.rejects(
      f.db.transaction(async (tx) => {
        const run =
          method === "execute"
            ? (id, value, rollback) =>
                tx.execute(`INSERT ${rollback ? "OR ROLLBACK" : ""} INTO items VALUES(?,?)`, [
                  id,
                  value,
                ])
            : await (async () => {
                const bad = await tx.prepare("INSERT OR ROLLBACK INTO items VALUES(?,?)");
                const good = await tx.prepare("INSERT INTO items VALUES(?,?)");
                return (id, value, rollback) => (rollback ? bad : good).execute([id, value]);
              })();
        // Both are admitted before the first response can reach the SDK.
        outcomes = await Promise.allSettled([run(2, "seed", true), run(3, "escaped", false)]);
      }),
    );
    assert.equal(outcomes[1].status, "rejected");
    assert.equal(outcomes[1].reason.code, "ERR_FSQLITE_TRANSACTION_ABORTED");
    assert.deepEqual(diskRows(f), [[1, "seed"]]);
    await assert.rejects(f.db.execute("INSERT INTO items VALUES(4,'future')"));
    assert.equal(f.worker.terminateCount, 1);
  });
}

test("managed scripts are preflighted in full before any SQL executes", async () => {
  const f = await fixture();
  await assert.rejects(
    f.db.transaction(async (tx) => {
      f.events.length = 0;
      await tx.executeBatch("INSERT INTO items VALUES(2,'tail'); --x\n COMMIT;");
    }),
    { code: sqlError },
  );
  assert.equal(
    f.events.some((sql) => sql.includes("'tail'")),
    false,
  );
  assert.deepEqual(await f.rows(), [[1, "seed"]]);
  await f.db.close();
});

test("managed scripts execute DDL and trigger bodies within their owned scope", async () => {
  const f = await fixture();
  await f.db.transaction(async (tx) => {
    await tx.executeBatch(`CREATE TABLE audit(value TEXT);
      CREATE TEMP TRIGGER [audit;change] AFTER INSERT ON items BEGIN
        INSERT INTO audit VALUES(CASE WHEN new.id > 0 THEN 'END;COMMIT' ELSE '' END);
        UPDATE audit SET value=value || ';done';
      END;
      INSERT INTO items VALUES(2,'trigger');`);
    assert.deepEqual((await tx.query("SELECT value FROM audit")).rowArrays, [["END;COMMIT;done"]]);
  });
  assert.deepEqual(await f.rows(), [
    [1, "seed"],
    [2, "trigger"],
  ]);
  await f.db.close();
});

test("each nested callback uses its own worker scope and preserves successful siblings", async () => {
  const f = await fixture();
  await f.db.transaction(async (parent) => {
    await parent.execute("INSERT INTO items VALUES(2,'parent')");
    await parent.transaction(async (child) => {
      await child.executeMany("INSERT INTO items VALUES(?,?)", [[3, "sibling"]]);
    });
    await assert.rejects(
      parent.transaction(async (child) => {
        await child.execute("INSERT INTO items VALUES(4,'discarded')");
        await child.execute("COMMIT");
      }),
      { code: sqlError },
    );
    await parent.execute("INSERT INTO items VALUES(5,'after')");
  });
  assert.deepEqual(await f.rows(), [
    [1, "seed"],
    [2, "parent"],
    [3, "sibling"],
    [5, "after"],
  ]);
  const requests = f.worker.requests;
  const begin = requests.filter((r) => r.kind === "transaction" && r.action === "begin");
  assert.equal(begin.length, 3);
  assert.equal(new Set(begin.map((r) => r.transactionId)).size, 3);
  assert.equal(begin[1].parentId, begin[0].transactionId);
  assert.equal(begin[2].parentId, begin[0].transactionId);
  assert.equal(
    requests.find((r) => r.kind === "execute-many").transactionId,
    begin[1].transactionId,
  );
  await f.db.close();
});

test("streaming import carries a stable worker scope through every chunk and cleanup", async () => {
  const f = await fixture();
  const result = await f.db.executeStream(
    "INSERT INTO items VALUES(?,?)",
    Array.from({ length: 513 }, (_, i) => [i + 2, `row${i}`]),
    { batchSize: 64 },
  );
  assert.equal(result.executions, 513);
  const begin = f.worker.requests.find((r) => r.kind === "transaction" && r.action === "begin");
  assert.ok(begin, "stream must begin through the explicit transaction protocol");
  const operations = f.worker.requests.filter((r) =>
    ["prepare", "statement-execute-many", "statement-finalize"].includes(r.kind),
  );
  assert.equal(operations.length, 11);
  assert.ok(operations.every((r) => r.transactionId === begin.transactionId));
  assert.equal((await f.rows()).length, 514);
  await f.db.close();
});

test("a failed managed BEGIN leaves a caller manual transaction intact", async () => {
  const f = await fixture();
  await f.db.executeBatch("BEGIN; INSERT INTO items VALUES(2,'manual')");
  let callbackRan = false;
  await assert.rejects(
    f.db.transaction(() => {
      callbackRan = true;
    }),
  );
  assert.equal(callbackRan, false);
  await f.db.executeBatch("COMMIT");
  assert.deepEqual(await f.rows(), [
    [1, "seed"],
    [2, "manual"],
  ]);
  await f.db.close();
});

test("a stale transaction request is refused at the real worker boundary", async () => {
  const f = await fixture();
  await f.db.transaction((tx) => tx.execute("INSERT INTO items VALUES(2,'first')"));
  const old = f.worker.requests.find((r) => r.kind === "transaction" && r.action === "begin");
  assert.ok(old);
  await f.db.transaction(async (tx) => {
    const replay = await f.host.handle({
      kind: "execute",
      requestId: 100000,
      transactionId: old.transactionId,
      sql: "INSERT INTO items VALUES(3,'replayed')",
    });
    assert.equal(replay.error.code, "ERR_FSQLITE_TRANSACTION_OWNERSHIP");
    await tx.execute("INSERT INTO items VALUES(4,'current')");
  });
  assert.deepEqual(await f.rows(), [
    [1, "seed"],
    [2, "first"],
    [4, "current"],
  ]);
  await f.db.close();
});

test("deferred commit failure rolls back and does not surrender ownership early", async () => {
  const f = await fixture();
  await f.db.executeBatch(
    "PRAGMA foreign_keys=ON; CREATE TABLE p(id PRIMARY KEY); CREATE TABLE c(v REFERENCES p DEFERRABLE INITIALLY DEFERRED)",
  );
  await assert.rejects(
    f.db.transaction((tx) => tx.execute("INSERT INTO c VALUES(1)")),
    /FOREIGN KEY/,
  );
  assert.deepEqual((await f.db.query("SELECT * FROM c")).rowArrays, []);
  await f.db.transaction((tx) => tx.execute("INSERT INTO items VALUES(2,'reused')"));
  assert.equal((await f.rows()).length, 2);
  await f.db.close();
});

function deferred() {
  let resolve;
  const promise = new Promise((yes) => {
    resolve = yes;
  });
  return { promise, resolve };
}
function hasCode(error, code) {
  return (
    error?.code === code ||
    (error?.errors ?? []).some((e) => hasCode(e, code)) ||
    (error?.cause !== undefined && error.cause !== error && hasCode(error.cause, code))
  );
}
const transactionCancelled = (error) => hasCode(error, "ERR_FSQLITE_TRANSACTION_CANCELLED");
const pauseOptions = { timeout: 8000 };

// These run through the production protocol, not a request-order-only mock.
test("transaction cancellation before BEGIN admits no database work", async () => {
  const f = await fixture(),
    controller = new AbortController();
  const reason = new Error("cancelled by caller");
  controller.abort(reason);
  const before = f.worker.requests.length;
  await assert.rejects(
    f.db.transaction(
      () => {
        throw new Error("callback ran");
      },
      { signal: controller.signal },
    ),
    (error) => error.code === "ERR_FSQLITE_TRANSACTION_CANCELLED" && error.cause === reason,
  );
  assert.equal(f.worker.requests.length, before);
  assert.deepEqual(await f.rows(), [[1, "seed"]]);
  await f.db.close();
});

test(
  "cancellation during BEGIN waits for its result then rolls back without running the callback",
  pauseOptions,
  async () => {
    const started = deferred(),
      release = deferred(),
      controller = new AbortController();
    const f = await fixture({
      async beforeBatch(sql) {
        if (sql === "BEGIN") {
          started.resolve();
          await release.promise;
        }
      },
    });
    let callbackRan = false,
      settled = false;
    const pending = f.db.transaction(
      () => {
        callbackRan = true;
      },
      { signal: controller.signal },
    );
    const checked = assert.rejects(pending, transactionCancelled);
    void pending.then(
      () => {
        settled = true;
      },
      () => {
        settled = true;
      },
    );
    await started.promise;
    controller.abort();
    assert.equal(settled, false);
    release.resolve();
    await checked;
    assert.equal(callbackRan, false);
    assert.deepEqual(await f.rows(), [[1, "seed"]]);
    await f.db.close();
  },
);

test(
  "out-of-band transaction cancellation fences queued writes while the current statement is suspended",
  pauseOptions,
  async () => {
    const started = deferred(),
      release = deferred(),
      controller = new AbortController();
    const f = await fixture({
      async beforeExecute(sql, params) {
        if (params[0] === 2) {
          started.resolve();
          await release.promise;
        }
      },
    });
    let results,
      settled = false;
    const pending = f.db.transaction(
      async (tx) => {
        results = await Promise.allSettled([
          tx.execute("INSERT INTO items VALUES(?,?)", [2, "current"]),
          tx.execute("INSERT INTO items VALUES(?,?)", [3, "queued"]),
        ]);
      },
      { signal: controller.signal },
    );
    const checked = assert.rejects(pending, transactionCancelled);
    void pending.then(
      () => {
        settled = true;
      },
      () => {
        settled = true;
      },
    );
    await started.promise;
    controller.abort();
    // The synchronous control handler updates the worker's fence, not its SQL.
    for (let i = 0; i < 20; i++) await Promise.resolve();
    assert.equal(settled, false);
    release.resolve();
    await checked;
    assert.equal(results[1].status, "rejected");
    assert.equal(results[1].reason.code, "ERR_FSQLITE_TRANSACTION_CANCELLED");
    assert.deepEqual(await f.rows(), [[1, "seed"]]);
    await f.db.transaction((tx) => tx.execute("INSERT INTO items VALUES(4,'reuse')"));
    assert.equal((await f.rows()).length, 2);
    await f.db.close();
  },
);

test("transaction handles expose the inherited signal and reject new work after cancellation", async () => {
  const f = await fixture(),
    controller = new AbortController(),
    reason = new Error("stop");
  await assert.rejects(
    f.db.transaction(
      async (tx) => {
        assert.equal(tx.signal.aborted, false);
        controller.abort(reason);
        assert.equal(tx.signal.aborted, true);
        assert.equal(tx.signal.reason, reason);
        await assert.rejects(
          tx.execute("INSERT INTO items VALUES(2,'late')"),
          transactionCancelled,
        );
        await assert.rejects(
          tx.transaction(() => {}),
          transactionCancelled,
        );
      },
      { signal: controller.signal },
    ),
    transactionCancelled,
  );
  assert.deepEqual(await f.rows(), [[1, "seed"]]);
  await f.db.close();
});

test("cancelling a child transaction rolls back only the child and leaves parent and siblings usable", async () => {
  const f = await fixture(),
    controller = new AbortController();
  await f.db.transaction(async (parent) => {
    await parent.execute("INSERT INTO items VALUES(2,'parent')");
    await parent.transaction((child) => child.execute("INSERT INTO items VALUES(3,'sibling')"));
    await assert.rejects(
      parent.transaction(
        async (child) => {
          await child.execute("INSERT INTO items VALUES(4,'cancelled')");
          controller.abort();
        },
        { signal: controller.signal },
      ),
      transactionCancelled,
    );
    assert.equal(parent.signal.aborted, false);
    await parent.execute("INSERT INTO items VALUES(5,'after')");
  });
  assert.deepEqual(await f.rows(), [
    [1, "seed"],
    [2, "parent"],
    [3, "sibling"],
    [5, "after"],
  ]);
  await f.db.close();
});

test("parent cancellation propagates into descendants even when a child failure is caught", async () => {
  const f = await fixture(),
    controller = new AbortController();
  await assert.rejects(
    f.db.transaction(
      async (parent) => {
        await parent.execute("INSERT INTO items VALUES(2,'outer')");
        await assert.rejects(
          parent.transaction(async (child) => {
            await child.transaction(async (grandchild) => {
              await grandchild.execute("INSERT INTO items VALUES(3,'inner')");
              controller.abort();
              assert.equal(grandchild.signal.aborted, true);
            });
          }),
          transactionCancelled,
        );
        await assert.rejects(
          parent.execute("INSERT INTO items VALUES(4,'late')"),
          transactionCancelled,
        );
      },
      { signal: controller.signal },
    ),
    transactionCancelled,
  );
  assert.deepEqual(await f.rows(), [[1, "seed"]]);
  await f.db.close();
});

for (const prepared of [false, true]) {
  test(
    `transaction cancellation reaches an active ${prepared ? "prepared" : "direct"} bulk without per-call options`,
    pauseOptions,
    async () => {
      const started = deferred(),
        release = deferred(),
        controller = new AbortController();
      let executions = 0;
      const f = await fixture({
        async beforeExecute(sql, params) {
          if (params.length) executions++;
          if (params[0] === 4) {
            started.resolve();
            await release.promise;
          }
        },
      });
      const pending = f.db.transaction(
        async (tx) => {
          const rows = [
            [2, "two"],
            [3, "three"],
            [4, "four"],
            [5, "five"],
          ];
          if (prepared) await (await tx.prepare("INSERT INTO items VALUES(?,?)")).executeMany(rows);
          else await tx.executeMany("INSERT INTO items VALUES(?,?)", rows);
        },
        { signal: controller.signal },
      );
      const checked = assert.rejects(pending, transactionCancelled);
      await started.promise;
      controller.abort();
      release.resolve();
      await checked;
      assert.equal(executions, 3);
      assert.deepEqual(await f.rows(), [[1, "seed"]]);
      await f.db.close();
    },
  );
}

test(
  "a nested stream inherits parent cancellation and closes its suspended producer before rollback",
  pauseOptions,
  async () => {
    const f = await fixture(),
      controller = new AbortController(),
      started = deferred(),
      release = deferred();
    let pulled = 0,
      closed = false;
    async function* source() {
      try {
        for (let i = 2; i < 20; i++) {
          if (i === 7) {
            started.resolve();
            await release.promise;
          }
          pulled++;
          yield [i, `v${i}`];
        }
      } finally {
        closed = true;
      }
    }
    const pending = f.db.transaction(
      async (tx) => {
        await tx.execute("INSERT INTO items VALUES(20,'parent')");
        await tx.executeStream("INSERT INTO items VALUES(?,?)", source(), { batchSize: 2 });
      },
      { signal: controller.signal },
    );
    const checked = assert.rejects(pending, transactionCancelled);
    await started.promise;
    controller.abort();
    release.resolve();
    await checked;
    assert.equal(closed, true);
    assert.equal(pulled, 6);
    assert.deepEqual(await f.rows(), [[1, "seed"]]);
    await f.db.close();
  },
);

test("an abort from statement cleanup is still before the transaction commit boundary", async () => {
  const controller = new AbortController();
  const f = await fixture({
    statementFree() {
      controller.abort();
    },
  });
  await assert.rejects(
    f.db.transaction(
      async (tx) => {
        await (await tx.prepare("INSERT INTO items VALUES(?,?)")).execute([2, "cleanup"]);
      },
      { signal: controller.signal },
    ),
    transactionCancelled,
  );
  assert.deepEqual(await f.rows(), [[1, "seed"]]);
  await f.db.close();
});

test(
  "a late abort after COMMIT dispatch cannot relabel actual durable success",
  pauseOptions,
  async () => {
    const started = deferred(),
      release = deferred(),
      controller = new AbortController();
    const f = await fixture({
      async beforeBatch(sql) {
        if (sql === "COMMIT") {
          started.resolve();
          await release.promise;
        }
      },
    });
    const pending = f.db.transaction(
      async (tx) => {
        await tx.execute("INSERT INTO items VALUES(2,'committed')");
        return 17;
      },
      { signal: controller.signal },
    );
    await started.promise;
    controller.abort();
    const begin = f.worker.requests.find((r) => r.kind === "transaction" && r.action === "begin");
    const ack = await f.host.handle({
      kind: "cancel-transaction",
      requestId: 9999,
      targetTransactionId: begin.transactionId,
    });
    release.resolve();
    assert.equal(await pending, 17);
    assert.equal(ack.accepted, false);
    assert.deepEqual(await f.rows(), [
      [1, "seed"],
      [2, "committed"],
    ]);
    await f.db.close();
  },
);

test("cancellation rollback failures retain causes and make the connection unusable", async () => {
  const controller = new AbortController(),
    reason = new Error("cancel reason");
  const f = await fixture({
    beforeBatch(sql) {
      if (sql === "ROLLBACK") throw new Error("rollback failed");
    },
  });
  await assert.rejects(
    f.db.transaction(
      async (tx) => {
        await tx.execute("INSERT INTO items VALUES(2,'pending')");
        controller.abort(reason);
      },
      { signal: controller.signal },
    ),
    (error) => {
      assert.ok(transactionCancelled(error));
      assert.ok(hasCode(error, "ERR_FSQLITE_TRANSACTION_CONNECTION_UNUSABLE"));
      return true;
    },
  );
  await assert.rejects(f.db.query("SELECT 1"));
  assert.equal(f.worker.terminateCount, 1);
});

test("invalid signal options cannot strand connection ownership", async () => {
  const f = await fixture();
  await assert.rejects(
    f.db.transaction(() => {}, { signal: { aborted: false } }),
    TypeError,
  );
  await f.db.transaction((tx) => tx.execute("INSERT INTO items VALUES(2,'valid')"));
  assert.deepEqual(await f.rows(), [
    [1, "seed"],
    [2, "valid"],
  ]);
  await f.db.close();
});

test(
  "cancellation delivery failure does not abandon cleanup or permit commit",
  pauseOptions,
  async () => {
    const f = await fixture(),
      controller = new AbortController(),
      started = deferred(),
      release = deferred();
    const post = f.worker.postMessage.bind(f.worker);
    f.worker.postMessage = (request, transfer) => {
      if (request.kind === "cancel-transaction") throw new Error("control channel failure");
      post(request, transfer);
    };
    const pending = f.db.transaction(
      async (tx) => {
        await tx.execute("INSERT INTO items VALUES(2,'rollback')");
        started.resolve();
        await release.promise;
      },
      { signal: controller.signal },
    );
    const checked = assert.rejects(pending, transactionCancelled);
    await started.promise;
    controller.abort();
    release.resolve();
    await checked;
    assert.equal(f.db.requestQueue.pendingRequests, 0);
    assert.deepEqual(await f.rows(), [[1, "seed"]]);
    await f.db.close();
  },
);

test("a stale cancellation has no effect on the next transaction and retains no future token", async () => {
  const f = await fixture();
  await f.db.transaction(() => {});
  const prior = f.worker.requests.find(
    (r) => r.kind === "transaction" && r.action === "begin",
  ).transactionId;
  const missing = await f.host.handle({
    kind: "cancel-transaction",
    requestId: 8888,
    targetTransactionId: "999",
  });
  assert.equal(missing.accepted, false);
  await f.db.transaction(async (tx) => {
    const ack = await f.host.handle({
      kind: "cancel-transaction",
      requestId: 9999,
      targetTransactionId: prior,
    });
    assert.equal(ack.accepted, false);
    await tx.execute("INSERT INTO items VALUES(2,'untouched')");
  });
  assert.deepEqual(await f.rows(), [
    [1, "seed"],
    [2, "untouched"],
  ]);
  await f.db.close();
});

// Actual Node worker-thread boundary. The worker uses the production host and
// real SQLite files; only the core SQL implementation is the Node reference.
async function threadedManaged(t, config = {}, limit = 4) {
  const { Worker } = await import("node:worker_threads");
  const core = new URL("../../worker/tests/helpers/snapshot-sqlite-core.mjs", import.meta.url).href;
  const script = `
    import {parentPort,workerData} from 'node:worker_threads';
    import {sqliteSnapshotWorker} from ${JSON.stringify(core)};
    let resume; const gate=new Promise(yes=>{resume=yes;});
    const pause=async()=>{parentPort.postMessage({fixture:'paused'});await gate;};
    const f=sqliteSnapshotWorker({requestLimits:{maxPendingRequests:workerData.limit},
      async beforeExecute(sql,params){if(workerData.config.row!==undefined&&params[0]===workerData.config.row)await pause();},
      async beforeBatch(sql){
        if(workerData.config.commit&&sql==='COMMIT')await pause();
        if(workerData.config.childBegin&&sql.startsWith('SAVEPOINT fsqlite_owned_'))await pause();
      }
    });
    parentPort.on('message',request=>{
      if(request.fixture==='resume'){resume();return;}
      void f.host.handle(request).then(response=>parentPort.postMessage(response),error=>{throw error;});
    });`;
  const native = new Worker(new URL(`data:text/javascript,${encodeURIComponent(script)}`), {
    execArgv: process.execArgv,
    workerData: { config, limit },
  });
  t.after(() => native.terminate());
  const paused = deferred(),
    ack = deferred(),
    wrappers = new Map(),
    requests = [];
  native.on("message", (message) => {
    if (message.fixture === "paused") paused.resolve();
    if (message.kind === "cancel-transaction-result") ack.resolve(message.accepted);
  });
  const worker = {
    addEventListener(type, listener) {
      const wrapped =
        type === "message"
          ? (data) => {
              if (data.kind) listener({ data });
            }
          : (error) => listener({ message: error.message });
      wrappers.set(listener, wrapped);
      native.on(type, wrapped);
    },
    removeEventListener(type, listener) {
      const wrapped = wrappers.get(listener);
      if (wrapped) native.off(type, wrapped);
      wrappers.delete(listener);
    },
    postMessage(request, transfer) {
      requests.push(request);
      native.postMessage(request, transfer);
    },
    terminate() {
      void native.terminate();
    },
  };
  const db = await FrankenDB.open({ worker, requestLimits: { maxPendingRequests: limit } });
  await db.execute("CREATE TABLE items(id INTEGER PRIMARY KEY,value TEXT)");
  return {
    db,
    requests,
    worker,
    paused: paused.promise,
    ack: ack.promise,
    resume: () => native.postMessage({ fixture: "resume" }),
  };
}

test(
  "real thread: whole-transaction cancel bypasses saturated admission and undoes trigger writes",
  pauseOptions,
  async (t) => {
    const f = await threadedManaged(t, { row: 4 }, 1),
      controller = new AbortController();
    await f.db.executeBatch(
      "CREATE TABLE audit(id);CREATE TRIGGER a AFTER INSERT ON items BEGIN INSERT INTO audit VALUES(new.id);END;",
    );
    const pending = f.db.transaction(
      (tx) =>
        tx.executeMany("INSERT INTO items VALUES(?,?)", [
          [2, "a"],
          [3, "b"],
          [4, "c"],
          [5, "d"],
        ]),
      { signal: controller.signal },
    );
    const checked = assert.rejects(pending, transactionCancelled);
    await f.paused;
    assert.equal(f.db.requestQueue.pendingRequests, 1);
    controller.abort();
    assert.equal(await f.ack, true);
    f.resume();
    await checked;
    assert.deepEqual((await f.db.query("SELECT * FROM items")).rowArrays, []);
    assert.deepEqual((await f.db.query("SELECT * FROM audit")).rowArrays, []);
    const path = f.db.path;
    await f.db.close();
    const disk = new DatabaseSync(path);
    try {
      assert.equal(disk.prepare("SELECT count(*) AS n FROM items").get().n, 0);
      assert.equal(disk.prepare("PRAGMA integrity_check").get().integrity_check, "ok");
    } finally {
      disk.close();
    }
  },
);

test(
  "real thread: parent abort while child BEGIN is suspended fences the newly created child",
  pauseOptions,
  async (t) => {
    const f = await threadedManaged(t, { childBegin: true }),
      controller = new AbortController();
    let childRan = false;
    const pending = f.db.transaction(
      async (parent) => {
        await parent.execute("INSERT INTO items VALUES(1,'parent')");
        await parent.transaction((child) => {
          childRan = true;
          return child.execute("INSERT INTO items VALUES(2,'child')");
        });
      },
      { signal: controller.signal },
    );
    const checked = assert.rejects(pending, transactionCancelled);
    await f.paused;
    controller.abort();
    assert.equal(await f.ack, true);
    f.resume();
    await checked;
    assert.equal(childRan, false);
    assert.deepEqual((await f.db.query("SELECT * FROM items")).rowArrays, []);
    await f.db.close();
  },
);

test(
  "real thread: commit-dispatched scope refuses late cancellation and returns durable success",
  pauseOptions,
  async (t) => {
    const f = await threadedManaged(t, { commit: true }),
      controller = new AbortController();
    const pending = f.db.transaction(
      async (tx) => {
        await tx.execute("INSERT INTO items VALUES(1,'durable')");
        return 42;
      },
      { signal: controller.signal },
    );
    await f.paused;
    controller.abort();
    const id = f.requests.find(
      (r) => r.kind === "transaction" && r.action === "begin",
    ).transactionId;
    f.worker.postMessage({ kind: "cancel-transaction", requestId: 9999, targetTransactionId: id });
    assert.equal(await f.ack, false);
    f.resume();
    assert.equal(await pending, 42);
    const path = f.db.path;
    await f.db.close();
    const disk = new DatabaseSync(path);
    try {
      assert.deepEqual(disk.prepare("SELECT id,value FROM items").all().map(Object.values), [
        [1, "durable"],
      ]);
    } finally {
      disk.close();
    }
  },
);
