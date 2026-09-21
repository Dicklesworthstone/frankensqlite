// Production SDK/worker/journal; file-backed Node SQLite is a reference core,
// not FrankenSQLite WASM. Exact SELECT counting is independent of journal SQL.

import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import { setTimeout as delay } from "node:timers/promises";
import { sqliteSnapshotWorker } from "../../worker/tests/helpers/snapshot-sqlite-core.mjs";
import { watchQuery } from "../src/index.ts";
import { FrankenDBQueue } from "../src/queue.ts";
import { deferred, observe } from "./helpers/controlled-worker.ts";

const options = { timeout: 10000 };
const SELECT = "SELECT id, value FROM items ORDER BY id";
const code = (name) => (error) =>
  error?.code === name || error?.cause?.code === name || error?.errors?.some(code(name));
const messageInTree = (error, text) =>
  String(error?.message ?? error).includes(text) ||
  (error?.cause !== undefined && messageInTree(error.cause, text)) ||
  error?.errors?.some((e) => messageInTree(e, text));

async function fixture(t, queueOptions = {}) {
  const f = sqliteSnapshotWorker();
  const queue = await FrankenDBQueue.open({ worker: f.worker }, queueOptions);
  t.after(() => queue.close().catch(() => {}));
  await queue.transaction((tx) =>
    tx.executeBatch(
      "CREATE TABLE items(id INTEGER PRIMARY KEY,value TEXT); CREATE TABLE other(id);",
    ),
  );
  return {
    ...f,
    queue,
    reads(sql = SELECT) {
      return f.worker.requests.filter((r) => r.kind === "query" && r.sql === sql).length;
    },
    async watch(sql = SELECT, config = {}) {
      const query = await watchQuery(queue, sql, { tables: ["items"], ...config });
      t.after(() => query.return().catch(() => {}));
      return query;
    },
  };
}
const rows = (result) => result.value.rowArrays;

test(
  "registration precedes a lazy initial read, including a write before first demand",
  options,
  async (t) => {
    const f = await fixture(t),
      q = f.queue;
    await q.transaction((tx) => tx.execute("INSERT INTO items VALUES(1,'existing')"));
    const live = await f.watch();
    assert.equal(f.reads(), 0);
    await q.transaction((tx) => tx.execute("INSERT INTO items VALUES(2,'after registration')"));
    assert.equal(f.reads(), 0);
    const first = await live.next();
    assert.equal(first.done, false);
    assert.deepEqual(rows(first), [
      [1, "existing"],
      [2, "after registration"],
    ]);
    assert.equal(first.value.throughSequence, 1n);
    assert.equal(f.reads(), 1);
    assert.ok(Object.isFrozen(first.value));
    assert.ok(Object.isFrozen(live));
    assert.deepEqual(live.tables, ["items"]);
  },
);

test(
  "late notification for a commit already included by the read does not requery",
  options,
  async (t) => {
    const f = await fixture(t),
      q = f.queue,
      live = await f.watch();
    // Notification delivery is a later task; this read already includes the write.
    await q.transaction((tx) => tx.execute("INSERT INTO items VALUES(1,'seen')"));
    assert.equal((await live.next()).value.throughSequence, 1n);
    const next = observe(live.next());
    await delay(15);
    assert.equal(next.outcome.status, "pending");
    assert.equal(f.reads(), 1);
    await live.return();
    await next.settled;
    assert.equal(next.outcome.value.done, true);
  },
);

test(
  "matching commits wake demand; reads, unrelated writes, no-op writes and rollback do not",
  options,
  async (t) => {
    const f = await fixture(t),
      q = f.queue,
      live = await f.watch();
    await live.next();
    const next = observe(live.next());
    await q.transaction(async (tx) => {
      await tx.query("SELECT * FROM items");
      await tx.execute("INSERT INTO other VALUES(1)");
      await tx.execute("DELETE FROM items WHERE id=999");
    });
    await assert.rejects(
      q.transaction(async (tx) => {
        await tx.execute("INSERT INTO items VALUES(1,'lost')");
        throw Error("rollback");
      }),
      /rollback/,
    );
    await delay(15);
    assert.equal(next.outcome.status, "pending");
    assert.equal(f.reads(), 1);
    await q.transaction((tx) => tx.execute("INSERT INTO items VALUES(2,'kept')"));
    await next.settled;
    assert.deepEqual(rows(next.outcome.value), [[2, "kept"]]);
    assert.equal(f.reads(), 2);
  },
);

test(
  "slow consumer causes no polling or result backlog across 100 committed changes",
  options,
  async (t) => {
    const f = await fixture(t),
      q = f.queue,
      live = await f.watch();
    await live.next();
    for (let i = 1; i <= 100; i++)
      await q.transaction((tx) => tx.execute("INSERT INTO items VALUES(?,?)", [i, `v${i}`]));
    await delay(10);
    assert.equal(f.reads(), 1);
    assert.equal(q.stats.pendingJobs, 0);
    const next = await live.next();
    assert.equal(f.reads(), 2);
    assert.equal(next.value.throughSequence, 100n);
    assert.deepEqual(
      rows(next),
      Array.from({ length: 100 }, (_, i) => [i + 1, `v${i + 1}`]),
    );
  },
);

test(
  "trigger dependencies and aggregates are read from one completed transaction",
  options,
  async (t) => {
    const f = await fixture(t),
      q = f.queue;
    await q.transaction((tx) =>
      tx.execute(
        "CREATE TRIGGER mirror AFTER INSERT ON other BEGIN INSERT INTO items VALUES(new.id,'trigger'); END;",
      ),
    );
    const live = await f.watch("SELECT count(*) AS total FROM items");
    assert.deepEqual(rows(await live.next()), [[0]]);
    const next = live.next();
    await q.transaction((tx) => tx.executeMany("INSERT INTO other VALUES(?)", [[1], [2], [3]]));
    assert.deepEqual(rows(await next), [[3]]);
  },
);

test(
  "one pending next is enforced before SQL and for-await break releases the watch",
  options,
  async (t) => {
    const f = await fixture(t),
      live = await f.watch();
    await live.next();
    const next = live.next();
    for (let i = 0; i < 12; i++)
      await assert.rejects(live.next(), code("ERR_FSQLITE_LIVE_QUERY_NEXT_PENDING"));
    assert.equal(f.reads(), 1);
    await live.return();
    assert.equal((await next).done, true);
    await live.done;
    const other = await f.watch();
    for await (const value of other) {
      assert.deepEqual(value.rowArrays, []);
      break;
    }
    assert.equal(other.closed, true);
    assert.equal(f.queue.stats.subscriptions, 0);
  },
);

test(
  "return joins a suspended real query and rollback before releasing connection ownership",
  options,
  async (t) => {
    const f = await fixture(t),
      q = f.queue,
      live = await f.watch(),
      entered = deferred(),
      release = deferred();
    const core = f.handles[0],
      query = core.query.bind(core);
    core.query = async (sql) => {
      if (sql === SELECT) {
        entered.resolve();
        await release.promise;
      }
      return query(sql);
    };
    const read = observe(live.next());
    await entered.promise;
    const close = observe(live.return());
    let successorRan = false;
    const successor = q.transaction((tx) => {
      successorRan = true;
      return tx.execute("INSERT INTO items VALUES(1,'after cleanup')");
    });
    await delay(5);
    assert.equal(close.outcome.status, "pending");
    assert.equal(read.outcome.status, "pending");
    assert.equal(successorRan, false);
    release.resolve();
    await Promise.all([read.settled, close.settled, successor]);
    assert.equal(read.outcome.status, "fulfilled");
    assert.equal(read.outcome.value.done, true);
    assert.equal(close.outcome.status, "fulfilled");
    assert.equal(live.closed, true);
    const disk = new DatabaseSync(q.path);
    t.after(() => disk.close());
    assert.deepEqual(disk.prepare("SELECT id,value FROM items").all().map(Object.values), [
      [1, "after cleanup"],
    ]);
    assert.deepEqual(disk.prepare("PRAGMA integrity_check").all().map(Object.values), [["ok"]]);
  },
);

test(
  "immediate next after lifetime abort ends normally and never submits an initial read",
  options,
  async (t) => {
    const f = await fixture(t),
      controller = new AbortController();
    controller.signal.addEventListener("abort", (event) => event.stopImmediatePropagation());
    const live = await f.watch(SELECT, { signal: controller.signal });
    controller.abort("unmount");
    assert.deepEqual(await live.next(), { done: true, value: undefined });
    await live.done;
    assert.equal(f.reads(), 0);
    assert.equal(f.queue.stats.subscriptions, 0);
  },
);

test("idle queue close settles pending demand without a matching write", options, async (t) => {
  const f = await fixture(t),
    live = await f.watch();
  await live.next();
  const next = live.next();
  await f.queue.close();
  assert.deepEqual(await next, { done: true, value: undefined });
  await live.done;
  assert.equal(live.closed, true);
});

test(
  "SQL failure terminates only its watch and preserves the queue and other observers",
  options,
  async (t) => {
    const f = await fixture(t),
      bad = await f.watch("SELECT missing_column FROM items"),
      good = await f.watch();
    let failure;
    await assert.rejects(bad.next(), (error) => {
      failure = error;
      return /missing_column/.test(error.message);
    });
    await assert.rejects(bad.done, (error) => error === failure);
    await assert.rejects(bad.next(), (error) => error === failure);
    assert.equal(bad.closed, true);
    assert.equal(f.queue.stats.subscriptions, 1);
    await f.queue.transaction((tx) => tx.execute("INSERT INTO items VALUES(1,'still working')"));
    assert.deepEqual(rows(await good.next()), [[1, "still working"]]);
  },
);

test("stopping a failed suspended read never hides the genuine SQL error", options, async (t) => {
  const f = await fixture(t),
    live = await f.watch(),
    entered = deferred(),
    release = deferred();
  const core = f.handles[0],
    query = core.query.bind(core);
  core.query = async (sql) => {
    if (sql === SELECT) {
      entered.resolve();
      await release.promise;
      throw Error("real query fault");
    }
    return query(sql);
  };
  const read = observe(live.next());
  await entered.promise;
  const close = observe(live.return());
  release.resolve();
  await Promise.all([read.settled, close.settled]);
  assert.equal(read.outcome.status, "rejected");
  assert.equal(close.outcome.status, "rejected");
  assert.ok(messageInTree(read.outcome.reason, "real query fault"));
  assert.equal(close.outcome.reason, read.outcome.reason);
  assert.equal(f.queue.stats.subscriptions, 0);
});

test(
  "query admission overload does not retry or silently skip the failed read",
  options,
  async (t) => {
    const f = await fixture(t, { maxPendingJobs: 1 }),
      live = await f.watch(),
      entered = deferred(),
      release = deferred();
    const held = f.queue.transaction(async () => {
      entered.resolve();
      await release.promise;
    });
    await entered.promise;
    await assert.rejects(live.next(), code("ERR_FSQLITE_JOB_QUEUE_FULL"));
    assert.equal(live.closed, true);
    await assert.rejects(live.done, code("ERR_FSQLITE_JOB_QUEUE_FULL"));
    assert.equal(f.reads(), 0);
    release.resolve();
    await held;
    assert.equal(f.queue.stats.subscriptions, 0);
  },
);

test("read start deadline removes queued work without executing SELECT", options, async (t) => {
  const f = await fixture(t),
    live = await f.watch(SELECT, { waitTimeoutMs: 20 }),
    entered = deferred(),
    release = deferred();
  const held = f.queue.transaction(async () => {
    entered.resolve();
    await release.promise;
  });
  await entered.promise;
  await assert.rejects(live.next(), code("ERR_FSQLITE_JOB_WAIT_TIMEOUT"));
  assert.equal(f.reads(), 0);
  await assert.rejects(live.done, code("ERR_FSQLITE_JOB_WAIT_TIMEOUT"));
  release.resolve();
  await held;
});

test(
  "unsafe replay shapes are refused before registering watches or executing SQL",
  options,
  async (t) => {
    const f = await fixture(t);
    const before = f.worker.requests.length;
    for (const sql of [
      "INSERT INTO items VALUES(1,'write') RETURNING id",
      "WITH x AS (SELECT 1) SELECT * FROM x",
      "PRAGMA user_version=42",
      "DELETE FROM items",
      "SELECT 1; DELETE FROM items",
      "SELECT 1\0; DELETE FROM items",
    ]) {
      await assert.rejects(f.watch(sql));
    }
    assert.equal(f.worker.requests.length, before);
    assert.equal(f.queue.stats.subscriptions, 0);
    const valid = await f.watch(
      "/* lead */\n; -- comment\nSELECT 'PRAGMA; DELETE' AS literal, ? AS n",
      { params: [7] },
    );
    assert.deepEqual(rows(await valid.next()), [["PRAGMA; DELETE", 7]]);
  },
);

test(
  "SQL parameters and blobs are privately captured before registration awaits",
  options,
  async (t) => {
    const f = await fixture(t),
      buffer = Uint8Array.of(1, 2, 3),
      params = [buffer, "initial"];
    const opening = f.watch("SELECT ? AS blob, ? AS text", { params });
    params[1] = "mutated";
    buffer.fill(9);
    const live = await opening;
    const first = await live.next();
    assert.deepEqual(rows(first), [[Uint8Array.of(1, 2, 3), "initial"]]);
    first.value.rowArrays[0][0].fill(8);
    await f.queue.transaction((tx) => tx.execute("INSERT INTO items VALUES(1,'change')"));
    await delay(5);
    assert.deepEqual(rows(await live.next()), [[Uint8Array.of(1, 2, 3), "initial"]]);
  },
);

test(
  "retained input budget checks full blob backings and rejects shared memory",
  options,
  async (t) => {
    const f = await fixture(t);
    await assert.rejects(
      f.watch("SELECT ?", { params: [new Uint8Array(8192).subarray(0, 1)], maxInputBytes: 1024 }),
      code("ERR_FSQLITE_REQUEST_TOO_LARGE"),
    );
    await assert.rejects(
      f.watch("SELECT ?", { params: [new Uint8Array(new SharedArrayBuffer(8))] }),
      code("ERR_FSQLITE_LIVE_QUERY_INPUT"),
    );
    for (const maxInputBytes of [0, 255, 1.5, Infinity, 64 * 1024 * 1024 + 1])
      await assert.rejects(
        f.watch(SELECT, { maxInputBytes }),
        code("ERR_FSQLITE_LIVE_QUERY_INPUT"),
      );
    assert.equal(f.queue.stats.subscriptions, 0);
    assert.equal(f.reads(), 0);
  },
);

test(
  "pre-aborted registration rejects without installing triggers or running the SELECT",
  options,
  async (t) => {
    const f = await fixture(t),
      controller = new AbortController();
    controller.abort("early");
    await assert.rejects(
      f.watch(SELECT, { signal: controller.signal }),
      code("ERR_FSQLITE_JOB_CANCELLED"),
    );
    assert.equal(f.queue.stats.subscriptions, 0);
    assert.equal(f.reads(), 0);
  },
);

test("consumer throw retains even undefined and settles a pending next", options, async (t) => {
  const f = await fixture(t),
    live = await f.watch();
  await live.next();
  const next = observe(live.next());
  let caught = false;
  await live.throw(undefined).catch((error) => {
    caught = true;
    assert.equal(error, undefined);
  });
  await next.settled;
  assert.equal(caught, true);
  assert.equal(next.outcome.status, "rejected");
  assert.equal(next.outcome.reason, undefined);
  assert.equal(live.closed, true);
  assert.equal(f.queue.stats.subscriptions, 0);
});

test(
  "writes queued during a read produce a later snapshot rather than being lost or mixed into it",
  options,
  async (t) => {
    const f = await fixture(t),
      q = f.queue;
    await q.transaction((tx) => tx.execute("INSERT INTO items VALUES(1,'before')"));
    const live = await f.watch(),
      entered = deferred(),
      release = deferred();
    let pause = true;
    const core = f.handles[0],
      query = core.query.bind(core);
    core.query = async (sql) => {
      if (pause && sql === SELECT) {
        pause = false;
        entered.resolve();
        await release.promise;
      }
      return query(sql);
    };
    const first = live.next();
    await entered.promise;
    const write = q.transaction((tx) => tx.execute("INSERT INTO items VALUES(2,'after')"));
    release.resolve();
    const snapshot = await first;
    assert.deepEqual(rows(snapshot), [[1, "before"]]);
    assert.equal(snapshot.value.throughSequence, 0n);
    await write;
    const next = await live.next();
    assert.deepEqual(rows(next), [
      [1, "before"],
      [2, "after"],
    ]);
    assert.equal(next.value.throughSequence, 1n);
    assert.equal(f.reads(), 2);
  },
);
