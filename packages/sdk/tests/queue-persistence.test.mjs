// Production queue, SDK, host and snapshot store. Persistence is an explicitly
// labeled transactional IndexedDB MODEL; SQL/export/import use real SQLite.

import assert from "node:assert/strict";
import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import {
  installIndexedDbModel,
  ModelObjectStore,
} from "../../worker/tests/helpers/indexeddb-model.mjs";
import { sqliteSnapshotWorker } from "../../worker/tests/helpers/snapshot-sqlite-core.mjs";
import { FrankenDBQueue } from "../src/index.ts";
import { deferred, drain, observe } from "./helpers/controlled-worker.ts";

const { databases } = installIndexedDbModel();
const limits = { timeout: 5000 };
async function open(name = crypto.randomUUID(), hooks = {}, options = {}) {
  const f = sqliteSnapshotWorker(hooks);
  const queue = await FrankenDBQueue.open(
    { worker: f.worker, persistence: "indexeddb-snapshot", dbName: name },
    options,
  );
  return { ...f, queue, name };
}
const rows = (q) =>
  q.transaction(async (tx) => (await tx.query("SELECT id,value FROM items ORDER BY id")).rowArrays);
const schema = (q) =>
  q.transaction((tx) => tx.execute("CREATE TABLE items(id INTEGER PRIMARY KEY,value TEXT UNIQUE)"));
const stored = (name) => databases.get(`frankensqlite:snapshot:v1:${name}`).values.get("head");
async function imageRows(bytes) {
  const dir = await mkdtemp(join(tmpdir(), "fsqlite-queued-image-")),
    path = join(dir, "image.sqlite");
  await writeFile(path, bytes);
  const db = new DatabaseSync(path);
  try {
    assert.deepEqual(db.prepare("PRAGMA integrity_check").all().map(Object.values), [["ok"]]);
    return db.prepare("SELECT id,value FROM items ORDER BY id").all().map(Object.values);
  } finally {
    db.close();
  }
}

test(
  "checkpoint is a FIFO publication barrier: prior commit included, later write excluded",
  limits,
  async () => {
    const entered = deferred(),
      release = deferred();
    let laterStarted = false;
    const a = await open(undefined, {
        beforeExport: async () => {
          entered.resolve();
          await release.promise;
        },
      }),
      q = a.queue;
    await schema(q);
    const first = q.transaction((tx) => tx.execute("INSERT INTO items VALUES(1,'saved')"));
    const saved = q.checkpoint();
    const later = q.transaction((tx) => {
      laterStarted = true;
      return tx.execute("INSERT INTO items VALUES(2,'not-saved')");
    });
    await entered.promise;
    await first;
    await drain();
    assert.equal(laterStarted, false);
    assert.equal(q.stats.waitingJobs, 1);
    release.resolve();
    const metadata = await saved;
    await later;
    assert.equal(q.snapshotRevision, metadata.revision);
    assert.equal(q.stats.pendingJobs, 0);
    const b = await open(a.name);
    assert.deepEqual(await rows(b.queue), [[1, "saved"]]);
    assert.deepEqual(await rows(q), [
      [1, "saved"],
      [2, "not-saved"],
    ]);
    await Promise.all([q.close(), b.queue.close()]);
  },
);

test(
  "close waits for the actual snapshot transaction completion acknowledgement",
  limits,
  async (t) => {
    const a = await open(),
      q = a.queue;
    await schema(q);
    await q.transaction((tx) => tx.execute("INSERT INTO items VALUES(1,'saved')"));
    const completed = deferred(),
      release = deferred(),
      original = ModelObjectStore.prototype.put;
    t.after(() => {
      ModelObjectStore.prototype.put = original;
      release.resolve();
    });
    ModelObjectStore.prototype.put = function (...args) {
      const tx = this.transaction,
        emit = tx.emit;
      tx.emit = (type, properties) => {
        if (type === "complete") {
          completed.resolve();
          void release.promise.then(() => emit.call(tx, type, properties));
        } else emit.call(tx, type, properties);
      };
      return original.apply(this, args);
    };
    const publication = observe(q.checkpoint());
    await completed.promise;
    const close = q.close(),
      closing = observe(close);
    await drain();
    assert.equal(publication.outcome.status, "pending");
    assert.equal(closing.outcome.status, "pending");
    assert.equal(a.worker.terminateCount, 0);
    assert.equal(q.snapshotRevision, null);
    release.resolve();
    await publication.settled;
    await close;
    assert.equal(publication.outcome.status, "fulfilled");
    assert.equal(a.worker.terminateCount, 1);
    assert.equal(q.snapshotRevision, publication.outcome.value.revision);
    ModelObjectStore.prototype.put = original;
    const b = await open(a.name);
    assert.deepEqual(await rows(b.queue), [[1, "saved"]]);
    await b.queue.close();
  },
);

test(
  "queued export takes one committed image without a transaction or later writes",
  limits,
  async () => {
    const entered = deferred(),
      release = deferred();
    let laterStarted = false;
    const f = sqliteSnapshotWorker({
      beforeExport: async () => {
        entered.resolve();
        await release.promise;
      },
    });
    const q = await FrankenDBQueue.open({ worker: f.worker });
    await schema(q);
    const before = q.transaction((tx) => tx.execute("INSERT INTO items VALUES(1,'before')"));
    const image = q.export();
    const after = q.transaction((tx) => {
      laterStarted = true;
      return tx.execute("INSERT INTO items VALUES(2,'after')");
    });
    await entered.promise;
    await before;
    assert.equal(laterStarted, false);
    // VACUUM INTO would fail if the scheduler had wrapped export in BEGIN.
    release.resolve();
    assert.deepEqual(await imageRows(await image), [[1, "before"]]);
    await after;
    assert.deepEqual(await rows(q), [
      [1, "before"],
      [2, "after"],
    ]);
    await q.close();
  },
);

test("an active export reports its real result despite a late abort", limits, async () => {
  const entered = deferred(),
    release = deferred(),
    c = new AbortController();
  const a = await open(undefined, {
      beforeExport: async () => {
        entered.resolve();
        await release.promise;
      },
    }),
    q = a.queue;
  await schema(q);
  await q.transaction((tx) => tx.execute("INSERT INTO items VALUES(1,'image')"));
  const pending = observe(q.export({ signal: c.signal }));
  await entered.promise;
  c.abort("too late");
  await drain();
  assert.equal(pending.outcome.status, "pending");
  assert.equal(q.stats.activeJobs, 1);
  release.resolve();
  await pending.settled;
  assert.equal(pending.outcome.status, "fulfilled");
  assert.deepEqual(await imageRows(pending.outcome.value), [[1, "image"]]);
  assert.equal(q.snapshotRevision, null);
  await q.close();
});

test(
  "active checkpoint cannot be relabeled cancelled after publication starts",
  limits,
  async () => {
    const entered = deferred(),
      release = deferred(),
      c = new AbortController();
    const a = await open(undefined, {
        beforeExport: async () => {
          entered.resolve();
          await release.promise;
        },
      }),
      q = a.queue;
    await schema(q);
    await q.transaction((tx) => tx.execute("INSERT INTO items VALUES(1,'saved')"));
    const pending = observe(q.checkpoint({ signal: c.signal }));
    await entered.promise;
    c.abort("late");
    await drain();
    assert.equal(pending.outcome.status, "pending");
    release.resolve();
    await pending.settled;
    assert.equal(pending.outcome.status, "fulfilled");
    assert.equal(stored(a.name).revision, pending.outcome.value.revision);
    const b = await open(a.name);
    assert.deepEqual(await rows(b.queue), [[1, "saved"]]);
    await Promise.all([q.close(), b.queue.close()]);
  },
);

for (const operation of ["checkpoint", "export"])
  test(`waiting ${operation} cancellation never exports or publishes`, limits, async () => {
    const a = await open(),
      q = a.queue;
    await schema(q);
    const hold = deferred(),
      entered = deferred(),
      c = new AbortController();
    const first = q.transaction(async () => {
      entered.resolve();
      await hold.promise;
    });
    await entered.promise;
    const count = a.worker.requests.length,
      pending = observe(q[operation]({ signal: c.signal }));
    c.abort("waiting");
    await pending.settled;
    assert.equal(pending.outcome.reason.code, "ERR_FSQLITE_JOB_CANCELLED");
    assert.equal(a.worker.requests.length, count);
    assert.equal(a.events.includes("export"), false);
    assert.equal(stored(a.name), undefined);
    hold.resolve();
    await first;
    await q.close();
  });

test(
  "barriers share job admission bounds and start deadlines without a hidden queue",
  limits,
  async () => {
    const a = await open(undefined, {}, { maxPendingJobs: 2 }),
      q = a.queue;
    await schema(q);
    const hold = deferred(),
      entered = deferred();
    const first = q.transaction(async () => {
      entered.resolve();
      await hold.promise;
    });
    await entered.promise;
    const timeout = observe(q.checkpoint({ waitTimeoutMs: 5 }));
    await assert.rejects(q.export(), { code: "ERR_FSQLITE_JOB_QUEUE_FULL" });
    await timeout.settled;
    assert.equal(timeout.outcome.reason.code, "ERR_FSQLITE_JOB_WAIT_TIMEOUT");
    assert.equal(a.events.includes("export"), false);
    const accepted = q.export();
    hold.resolve();
    await first;
    assert.deepEqual(await imageRows(await accepted), []);
    await q.close();
  },
);

test(
  "snapshot quota failure retains previous publication and in-memory data; later jobs continue",
  limits,
  async (t) => {
    const a = await open(),
      q = a.queue;
    await schema(q);
    const baseline = await q.checkpoint();
    const original = ModelObjectStore.prototype.put;
    let fail = true;
    ModelObjectStore.prototype.put = function (...args) {
      if (fail) {
        fail = false;
        return this.transaction.request(() => {
          throw new DOMException("quota exhausted", "QuotaExceededError");
        });
      }
      return original.apply(this, args);
    };
    t.after(() => {
      ModelObjectStore.prototype.put = original;
    });
    const write = q.transaction((tx) => tx.execute("INSERT INTO items VALUES(1,'memory')"));
    const failed = observe(q.checkpoint());
    const next = q.transaction((tx) => tx.execute("INSERT INTO items VALUES(2,'continued')"));
    await Promise.all([write, failed.settled, next]);
    assert.equal(failed.outcome.status, "rejected");
    assert.equal(q.snapshotRevision, baseline.revision);
    assert.equal(stored(a.name).revision, baseline.revision);
    assert.deepEqual(await rows(q), [
      [1, "memory"],
      [2, "continued"],
    ]);
    const retry = await q.checkpoint();
    assert.notEqual(retry.revision, baseline.revision);
    const b = await open(a.name);
    assert.deepEqual(await rows(b.queue), [
      [1, "memory"],
      [2, "continued"],
    ]);
    await Promise.all([q.close(), b.queue.close()]);
  },
);

test("export failure releases only that job and leaves the connection usable", limits, async () => {
  let fail = true;
  const a = await open(undefined, {
      beforeExport() {
        if (fail) throw new Error("export failed");
      },
    }),
    q = a.queue;
  await schema(q);
  const failed = observe(q.export());
  const next = q.transaction((tx) => tx.execute("INSERT INTO items VALUES(1,'kept')"));
  await Promise.all([failed.settled, next]);
  assert.match(failed.outcome.reason.message, /export failed/);
  fail = false;
  assert.deepEqual(await imageRows(await q.export()), [[1, "kept"]]);
  assert.equal(q.snapshotRevision, null);
  await q.close();
});

test(
  "competing queued sessions retain conflict detection rather than overwriting newer snapshots",
  limits,
  async () => {
    const a = await open(),
      q = a.queue;
    await schema(q);
    const baseline = await q.checkpoint();
    const b = await open(a.name);
    await q.transaction((tx) => tx.execute("INSERT INTO items VALUES(1,'winner')"));
    const winner = await q.checkpoint();
    await b.queue.transaction((tx) => tx.execute("INSERT INTO items VALUES(2,'local')"));
    await assert.rejects(b.queue.checkpoint(), { code: "ERR_FSQLITE_SNAPSHOT_CONFLICT" });
    assert.equal(b.queue.snapshotRevision, baseline.revision);
    assert.equal(stored(a.name).revision, winner.revision);
    assert.deepEqual(await imageRows(await b.queue.export()), [[2, "local"]]);
    const c = await open(a.name);
    assert.deepEqual(await rows(c.queue), [[1, "winner"]]);
    await Promise.all([q.close(), b.queue.close(), c.queue.close()]);
  },
);

test(
  "a barrier waits for failed predecessors to settle but does not turn independent jobs into one atomic batch",
  limits,
  async () => {
    const a = await open(),
      q = a.queue;
    await schema(q);
    const failed = observe(
      q.transaction(async (tx) => {
        await tx.execute("INSERT INTO items VALUES(1,'rolled-back')");
        throw new Error("fail");
      }),
    );
    const succeeded = q.transaction((tx) => tx.execute("INSERT INTO items VALUES(2,'saved')"));
    const checkpoint = q.checkpoint();
    await Promise.all([failed.settled, succeeded, checkpoint]);
    const b = await open(a.name);
    assert.deepEqual(await rows(b.queue), [[2, "saved"]]);
    await Promise.all([q.close(), b.queue.close()]);
  },
);

test("closing does not implicitly checkpoint successful queued SQL", limits, async () => {
  const a = await open(),
    q = a.queue;
  await schema(q);
  const baseline = await q.checkpoint();
  const write = q.transaction((tx) => tx.execute("INSERT INTO items VALUES(1,'unsaved')"));
  await Promise.all([write, q.close()]);
  const b = await open(a.name);
  assert.equal(b.queue.snapshotRevision, baseline.revision);
  assert.deepEqual(await rows(b.queue), []);
  await b.queue.close();
});

test(
  "memory queues reject checkpoint without pretending to provide persistence and still export",
  limits,
  async () => {
    const f = sqliteSnapshotWorker(),
      q = await FrankenDBQueue.open({ worker: f.worker });
    await schema(q);
    await assert.rejects(q.checkpoint(), { code: "ERR_FSQLITE_SNAPSHOT_MODE" });
    await q.transaction((tx) => tx.execute("INSERT INTO items VALUES(1,'memory')"));
    assert.deepEqual(await imageRows(await q.export()), [[1, "memory"]]);
    await q.close();
  },
);

test(
  "closed admission refuses both barrier APIs and reentrant getters cannot bypass close",
  limits,
  async () => {
    const a = await open(),
      q = a.queue;
    await schema(q);
    let closing;
    await assert.rejects(
      q.checkpoint({
        get signal() {
          closing = q.close();
          return undefined;
        },
      }),
      { code: "ERR_FSQLITE_JOB_QUEUE_CLOSED" },
    );
    await closing;
    await assert.rejects(q.export(), { code: "ERR_FSQLITE_JOB_QUEUE_CLOSED" });
    await assert.rejects(q.checkpoint(), { code: "ERR_FSQLITE_JOB_QUEUE_CLOSED" });
    assert.equal(stored(a.name), undefined);
  },
);
