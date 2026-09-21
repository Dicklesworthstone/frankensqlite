// Real Node worker threads and real file-backed SQLite through production SDK/
// worker code. This does not execute a browser or FrankenSQLite WASM artifact.

import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import { Worker } from "node:worker_threads";
import { FrankenDBQueue } from "../src/index.ts";
import { drain, observe } from "./helpers/controlled-worker.ts";

async function threaded(t) {
  const core = new URL("../../worker/tests/helpers/snapshot-sqlite-core.mjs", import.meta.url).href;
  const script = `
    import {parentPort} from 'node:worker_threads';
    import {sqliteSnapshotWorker} from ${JSON.stringify(core)};
    let config={},resume;
    const pause=()=>new Promise(resolve=>{resume=resolve;parentPort.postMessage({fixture:'paused'});});
    const f=sqliteSnapshotWorker({
      async beforeExecute(sql,params){if(config.row!==undefined&&params[0]===config.row)await pause();},
      async beforeBatch(sql){if(config.commit&&sql==='COMMIT')await pause();}
    });
    parentPort.on('message',request=>{
      if(request.fixture==='configure'){config=request.config;parentPort.postMessage({fixture:'configured'});return;}
      if(request.fixture==='resume'){config={};resume();return;}
      if(request.fixture==='crash'){throw new Error('intentional queue test worker crash');}
      void f.host.handle(request).then(response=>parentPort.postMessage(response),error=>{throw error;});
    });`;
  const native = new Worker(new URL(`data:text/javascript,${encodeURIComponent(script)}`), {
    execArgv: process.execArgv,
  });
  t.after(() => native.terminate());
  const history = [],
    waiters = [],
    wrappers = new Map();
  native.on("message", (message) => {
    history.push(message);
    for (const waiter of [...waiters])
      if (waiter.matches(message)) {
        waiters.splice(waiters.indexOf(waiter), 1);
        waiter.resolve(message);
      }
  });
  native.on("error", (error) => {
    for (const waiter of waiters.splice(0)) waiter.reject(error);
  });
  const until = (matches) => {
    const found = history.find(matches);
    if (found) return Promise.resolve(found);
    return new Promise((resolve, reject) => waiters.push({ matches, resolve, reject }));
  };
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
      native.postMessage(request, transfer);
    },
    terminate() {
      void native.terminate();
    },
  };
  const queue = await FrankenDBQueue.open(
    { worker, requestLimits: { maxPendingRequests: 1 } },
    { maxPendingJobs: 32 },
  );
  await queue.transaction((tx) =>
    tx.execute("CREATE TABLE items(id INTEGER PRIMARY KEY,value TEXT UNIQUE)"),
  );
  return {
    queue,
    native,
    async configure(config) {
      history.length = 0;
      native.postMessage({ fixture: "configure", config });
      await until((m) => m.fixture === "configured");
    },
    paused: () => until((m) => m.fixture === "paused"),
    cancelAck: () => until((m) => m.kind === "cancel-transaction-result"),
    resume: () => native.postMessage({ fixture: "resume" }),
    crash: () => native.postMessage({ fixture: "crash" }),
  };
}
function onDisk(path) {
  const db = new DatabaseSync(path);
  try {
    assert.deepEqual(db.prepare("PRAGMA integrity_check").all().map(Object.values), [["ok"]]);
    return db.prepare("SELECT id,value FROM items ORDER BY id").all().map(Object.values);
  } finally {
    db.close();
  }
}
const options = { timeout: 15000 };

test(
  "real thread: concurrent callers serialize full transactions through a one-request transport",
  options,
  async (t) => {
    const f = await threaded(t),
      q = f.queue;
    const jobs = Array.from({ length: 20 }, (_, id) =>
      q.transaction(async (tx) => {
        assert.equal((await tx.query("SELECT count(*) AS n FROM items")).rows[0].n, id);
        await tx.execute("INSERT INTO items VALUES(?,?)", [id, `v${id}`]);
        const statement = await tx.prepare("SELECT value FROM items WHERE id=?");
        return (await statement.get([id])).value;
      }),
    );
    const exported = q.export(),
      closed = q.close();
    assert.deepEqual(
      await Promise.all(jobs),
      Array.from({ length: 20 }, (_, id) => `v${id}`),
    );
    assert.ok((await exported).length >= 512);
    await closed;
    assert.deepEqual(
      onDisk(q.path),
      Array.from({ length: 20 }, (_, id) => [id, `v${id}`]),
    );
    assert.equal(q.stats.pendingJobs, 0);
    assert.equal(q.stats.completedJobs, 22);
    assert.equal(q.stats.state, "closed");
  },
);

test(
  "real thread: active cancellation waits for in-flight SQL and undoes trigger effects before the next job",
  options,
  async (t) => {
    const f = await threaded(t),
      q = f.queue,
      c = new AbortController();
    let laterRan = false;
    await q.transaction((tx) =>
      tx.executeBatch(
        "CREATE TABLE audit(id);CREATE TRIGGER a AFTER INSERT ON items BEGIN INSERT INTO audit VALUES(new.id);END;",
      ),
    );
    await f.configure({ row: 2 });
    const first = observe(
      q.transaction(
        async (tx) => {
          await tx.execute("INSERT INTO items VALUES(?,?)", [1, "before"]);
          await tx.execute("INSERT INTO items VALUES(?,?)", [2, "blocked"]);
        },
        { signal: c.signal },
      ),
    );
    const next = q.transaction((tx) => {
      laterRan = true;
      return tx.execute("INSERT INTO items VALUES(?,?)", [100, "after"]);
    });
    await f.paused();
    c.abort("cancel");
    assert.equal((await f.cancelAck()).accepted, true);
    await drain();
    assert.equal(first.outcome.status, "pending");
    assert.equal(laterRan, false);
    f.resume();
    await first.settled;
    await next;
    assert.equal(first.outcome.status, "rejected");
    assert.deepEqual((await q.transaction((tx) => tx.query("SELECT * FROM audit"))).rowArrays, [
      [100],
    ]);
    await q.close();
    assert.deepEqual(onDisk(q.path), [[100, "after"]]);
  },
);

test(
  "real thread: late abort cannot turn a committed job into failure or release the next job early",
  options,
  async (t) => {
    const f = await threaded(t),
      q = f.queue,
      c = new AbortController();
    let nextRan = false;
    await f.configure({ commit: true });
    const first = q.transaction(
      async (tx) => {
        await tx.execute("INSERT INTO items VALUES(1,'committed')");
        return 42;
      },
      { signal: c.signal },
    );
    const next = q.transaction((tx) => {
      nextRan = true;
      return tx.execute("INSERT INTO items VALUES(2,'next')");
    });
    await f.paused();
    c.abort("late");
    await drain();
    assert.equal(nextRan, false);
    f.resume();
    assert.equal(await first, 42);
    await next;
    await q.close();
    assert.deepEqual(onDisk(q.path), [
      [1, "committed"],
      [2, "next"],
    ]);
    assert.equal(q.stats.failedJobs, 0);
  },
);

test(
  "real thread: an actual worker crash settles pending jobs and rolls back the uncommitted file",
  options,
  async (t) => {
    const f = await threaded(t),
      q = f.queue;
    let called = false;
    await f.configure({ row: 2 });
    const first = observe(
      q.transaction(async (tx) => {
        await tx.execute("INSERT INTO items VALUES(?,?)", [1, "uncommitted"]);
        await tx.execute("INSERT INTO items VALUES(?,?)", [2, "blocked"]);
      }),
    );
    const second = observe(
      q.transaction(() => {
        called = true;
      }),
    );
    await f.paused();
    const exited = new Promise((resolve) => f.native.once("exit", resolve));
    f.crash();
    await Promise.all([first.settled, second.settled, exited]);
    assert.equal(first.outcome.status, "rejected");
    assert.equal(second.outcome.status, "rejected");
    assert.equal(called, false);
    await observe(q.close()).settled;
    assert.equal(q.stats.pendingJobs, 0);
    assert.equal(q.stats.state, "closed");
    assert.deepEqual(onDisk(q.path), []);
  },
);
