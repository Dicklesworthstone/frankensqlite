// Production public SDK + worker host + Node SQLite. Storage and Web Locks
// use deterministic models; this is not actual browser/WASM certification.

import assert from "node:assert/strict";
import { test } from "node:test";
import { installIndexedDbModel } from "../../worker/tests/helpers/indexeddb-model.mjs";
import { deferred, installOpfsModel } from "../../worker/tests/helpers/opfs-model.mjs";
import { sqliteSnapshotWorker } from "../../worker/tests/helpers/snapshot-sqlite-core.mjs";
import { FrankenDB } from "../src/database.ts";
import { FrankenWorkerClient } from "../src/worker-client.ts";

function fixture(t) {
  const storage = installOpfsModel();
  installIndexedDbModel();
  const sessions = [];
  t.after(async () => {
    for (const f of sessions) await f.host.handle({ kind: "close", requestId: 0 });
    storage.restore();
  });
  function session(hooks = {}) {
    const f = sqliteSnapshotWorker(hooks);
    sessions.push(f);
    return f;
  }
  async function open(options = {}, hooks = {}) {
    const f = session(hooks);
    const db = await FrankenDB.open({
      dbName: "db",
      persistence: "opfs-snapshot",
      snapshotOwnership: "exclusive",
      ...options,
      worker: f.worker,
    });
    return { ...f, db };
  }
  return { storage, open, session };
}

for (const persistence of ["indexeddb-snapshot", "opfs-snapshot"]) {
  test(`${persistence}: public exclusive open prevents a competing consumer before SQL`, async (t) => {
    const { open, session, storage } = fixture(t);
    const a = await open({ persistence });
    assert.equal(a.db.snapshotOwnership, "exclusive");
    await a.db.executeBatch(
      "CREATE TABLE jobs(id INTEGER PRIMARY KEY, claimed INTEGER); INSERT INTO jobs VALUES(1,0)",
    );
    await a.db.checkpoint();
    const competitor = session();
    await assert.rejects(
      FrankenDB.open({
        dbName: "db",
        persistence,
        snapshotOwnership: "exclusive",
        worker: competitor.worker,
      }),
      { code: "ERR_FSQLITE_SNAPSHOT_OWNED" },
    );
    assert.deepEqual(competitor.counts(), { creates: 0, imports: 0 });
    assert.equal(competitor.worker.terminateCount, 1);
    assert.equal(storage.sessions.size, 1);
    await a.db.transaction(async (tx) => {
      await tx.execute("UPDATE jobs SET claimed=1 WHERE id=1");
    });
    await a.db.checkpoint();
    await a.db.close();
    const b = await open({ persistence });
    assert.deepEqual((await b.db.query("SELECT * FROM jobs")).rows, [{ id: 1, claimed: 1 }]);
    await b.db.close();
    assert.equal(storage.sessions.size, 0);
  });

  test(`${persistence}: shared default stays compatible and cannot bypass an exclusive session`, async (t) => {
    const { open, storage } = fixture(t);
    const a = await open({ persistence, snapshotOwnership: undefined });
    const b = await open({ persistence, snapshotOwnership: "shared" });
    assert.equal(a.db.snapshotOwnership, "shared");
    assert.equal(b.db.snapshotOwnership, "shared");
    await assert.rejects(open({ persistence }), { code: "ERR_FSQLITE_SNAPSHOT_OWNED" });
    await a.db.close();
    await b.db.close();
    const exclusive = await open({ persistence });
    await assert.rejects(open({ persistence, snapshotOwnership: undefined }), {
      code: "ERR_FSQLITE_SNAPSHOT_OWNED",
    });
    await exclusive.db.close();
    assert.equal(storage.sessions.size, 0);
  });

  test(`${persistence}: exact-checkpoint reopening retains the explicitly requested ownership`, async (t) => {
    const { open } = fixture(t);
    const a = await open({ persistence });
    await a.db.executeBatch("CREATE TABLE items(v TEXT); INSERT INTO items VALUES('first')");
    const receipt = await a.db.checkpoint();
    await a.db.close();
    const requireCheckpoint = {
      path: "db",
      persistence,
      publicationId: receipt.revision,
      parentRevision: receipt.parentRevision,
    };
    const b = await open({ persistence, requireCheckpoint });
    assert.equal(b.db.snapshotOwnership, "exclusive");
    await assert.rejects(open({ persistence, requireCheckpoint }), {
      code: "ERR_FSQLITE_SNAPSHOT_OWNED",
    });
    assert.deepEqual((await b.db.query("SELECT * FROM items")).rows, [{ v: "first" }]);
    await b.db.execute("INSERT INTO items VALUES('second')");
    const next = await b.db.checkpoint();
    assert.equal(next.parentRevision, receipt.revision);
    await b.db.close();
  });
}

test("public close joins a pending checkpoint before allowing a new owner", async (t) => {
  const { open, storage } = fixture(t);
  const a = await open();
  await a.db.executeBatch("CREATE TABLE items(id INTEGER); INSERT INTO items VALUES(1)");
  const entered = deferred(),
    finish = deferred();
  storage.hooks.beforeClose = async () => {
    entered.resolve();
    await finish.promise;
  };
  const saving = a.db.checkpoint();
  await entered.promise;
  let closed = false;
  const closing = a.db.close().then(() => {
    closed = true;
  });
  await assert.rejects(open(), { code: "ERR_FSQLITE_SNAPSHOT_OWNED" });
  assert.equal(closed, false);
  finish.resolve();
  await saving;
  await closing;
  const b = await open();
  assert.deepEqual((await b.db.query("SELECT * FROM items")).rows, [{ id: 1 }]);
  await b.db.close();
});

test("uncertain OPFS checkpoint retains ownership through read-only recovery", async (t) => {
  const { open, storage } = fixture(t);
  const a = await open();
  await a.db.executeBatch("CREATE TABLE items(id INTEGER); INSERT INTO items VALUES(1)");
  storage.hooks.afterClose = () => {
    throw new Error("receipt lost");
  };
  await assert.rejects(a.db.checkpoint(), /receipt lost/);
  const identity = a.db.pendingCheckpointRecovery;
  assert.ok(identity);
  await assert.rejects(open(), { code: "ERR_FSQLITE_SNAPSHOT_OWNED" });
  storage.hooks.afterClose = undefined;
  const publications = storage.counts.publications;
  const recovered = await a.db.recoverCheckpoint();
  assert.equal(recovered.revision, identity.publicationId);
  assert.equal(storage.counts.publications, publications);
  assert.equal(a.db.snapshotOwnership, "exclusive");
  await a.db.close();
  const b = await open({ requireCheckpoint: identity });
  await b.db.close();
});

test("ownership options are captured before caller mutation or worker construction", async (t) => {
  const { session, storage } = fixture(t);
  const f = session();
  let reads = 0;
  const options = {
    dbName: "db",
    persistence: "opfs-snapshot",
    worker: () => {
      Object.defineProperty(options, "snapshotOwnership", { value: "shared", configurable: true });
      return f.worker;
    },
    get snapshotOwnership() {
      reads++;
      return "exclusive";
    },
  };
  const db = await FrankenDB.open(options);
  assert.equal(reads, 1);
  assert.equal(db.snapshotOwnership, "exclusive");
  assert.equal(f.worker.requests[0].config.snapshotOwnership, "exclusive");
  await db.close();
  assert.equal(storage.sessions.size, 0);
});

test("invalid policies reject before constructing a worker or detaching import bytes", async () => {
  let calls = 0;
  const worker = () => {
    calls++;
    throw new Error("must not construct");
  };
  const bytes = new Uint8Array(512);
  for (const snapshotOwnership of [null, 0, false, "typo"]) {
    await assert.rejects(
      FrankenDB.import(bytes, {
        dbName: "db",
        persistence: "opfs-snapshot",
        snapshotOwnership,
        worker,
      }),
      { code: "ERR_FSQLITE_SNAPSHOT_OWNERSHIP_INPUT" },
    );
  }
  await assert.rejects(FrankenDB.open({ snapshotOwnership: "exclusive", worker }), {
    code: "ERR_FSQLITE_SNAPSHOT_OWNERSHIP_INPUT",
  });
  assert.equal(calls, 0);
  assert.equal(bytes.byteLength, 512);
});

test("unavailable Web Locks never downgrade an explicit request", async (t) => {
  const { open, storage } = fixture(t);
  const locks = navigator.locks;
  navigator.locks = undefined;
  try {
    await assert.rejects(open({ persistence: "indexeddb-snapshot" }), {
      code: "ERR_FSQLITE_SNAPSHOT_OWNERSHIP_UNAVAILABLE",
    });
    const a = await open({ persistence: "indexeddb-snapshot", snapshotOwnership: undefined });
    assert.equal(a.db.snapshotOwnership, null);
    await a.db.close();
    assert.equal(storage.sessions.size, 0);
  } finally {
    navigator.locks = locks;
  }
});

function peer(ready) {
  const listeners = new Map();
  const requests = [];
  let terminations = 0;
  return {
    requests,
    get terminations() {
      return terminations;
    },
    addEventListener(type, listener) {
      if (!listeners.has(type)) listeners.set(type, new Set());
      listeners.get(type).add(listener);
    },
    removeEventListener(type, listener) {
      listeners.get(type)?.delete(listener);
    },
    terminate() {
      terminations++;
    },
    postMessage(request) {
      requests.push(request);
      queueMicrotask(() => {
        const response =
          request.kind === "init"
            ? { kind: "ready", requestId: request.requestId, data: ready(request.config) }
            : { kind: "close-result", requestId: request.requestId };
        for (const listener of listeners.get("message") ?? []) listener({ data: response });
      });
    },
  };
}

for (const held of [undefined, "shared", null, "wrong", false]) {
  test(`exclusive open refuses missing or downgraded acknowledgement: ${String(held)}`, async () => {
    const worker = peer((config) => ({
      path: config.dbName,
      persistence: config.persistence,
      ...(held === undefined ? {} : { snapshotOwnership: held }),
    }));
    await assert.rejects(
      FrankenDB.open({
        dbName: "db",
        persistence: "indexeddb-snapshot",
        snapshotOwnership: "exclusive",
        worker,
      }),
      { code: "ERR_FSQLITE_SNAPSHOT_OWNERSHIP_UNAVAILABLE" },
    );
    assert.equal(worker.terminations, 1);
    assert.deepEqual(
      worker.requests.map((r) => r.kind),
      ["init"],
    );
  });
}

test("an explicit shared policy is not treated as an unacknowledged legacy default", async () => {
  const worker = peer((config) => ({ path: config.dbName, persistence: config.persistence }));
  await assert.rejects(
    FrankenDB.open({
      dbName: "db",
      persistence: "indexeddb-snapshot",
      snapshotOwnership: "shared",
      worker,
    }),
    { code: "ERR_FSQLITE_SNAPSHOT_OWNERSHIP_UNAVAILABLE" },
  );
});

test("unsolicited exclusive ownership and ownership on memory are rejected", async () => {
  for (const persistence of ["memory", "indexeddb-snapshot"]) {
    const worker = peer((config) => ({
      path: config.dbName,
      persistence,
      snapshotOwnership: "exclusive",
    }));
    await assert.rejects(FrankenDB.open({ dbName: "db", persistence, worker }), {
      code: "ERR_FSQLITE_SNAPSHOT_OWNERSHIP_UNAVAILABLE",
    });
  }
});

test("inherited acknowledgement cannot satisfy exclusive policy", async () => {
  const worker = peer((config) =>
    Object.assign(Object.create({ snapshotOwnership: "exclusive" }), {
      path: config.dbName,
      persistence: config.persistence,
    }),
  );
  await assert.rejects(
    FrankenDB.open({
      dbName: "db",
      persistence: "indexeddb-snapshot",
      snapshotOwnership: "exclusive",
      worker,
    }),
    { code: "ERR_FSQLITE_SNAPSHOT_OWNERSHIP_UNAVAILABLE" },
  );
});

test("an acknowledgement getter is not invoked or trusted", async () => {
  let reads = 0;
  const worker = peer((config) => ({
    path: config.dbName,
    persistence: config.persistence,
    get snapshotOwnership() {
      reads++;
      return "exclusive";
    },
  }));
  await assert.rejects(
    FrankenDB.open({
      dbName: "db",
      persistence: "indexeddb-snapshot",
      snapshotOwnership: "exclusive",
      worker,
    }),
    { code: "ERR_FSQLITE_WORKER_RESPONSE" },
  );
  assert.equal(reads, 0);
  assert.equal(worker.terminations, 1);
});

test("legacy workers remain usable only when no ownership policy was requested", async () => {
  const worker = peer((config) => ({ path: config.dbName, persistence: config.persistence }));
  const db = await FrankenDB.open({ dbName: "db", persistence: "indexeddb-snapshot", worker });
  assert.equal(db.snapshotOwnership, null);
  await db.close();
});

test("direct client policy failure fences later SQL admission", async () => {
  const worker = peer((config) => ({ path: config.dbName, persistence: config.persistence }));
  const client = new FrankenWorkerClient(worker);
  await assert.rejects(
    client.init({
      dbName: "db",
      persistence: "indexeddb-snapshot",
      snapshotOwnership: "exclusive",
    }),
    { code: "ERR_FSQLITE_SNAPSHOT_OWNERSHIP_UNAVAILABLE" },
  );
  await assert.rejects(client.execute("SELECT 1"), {
    code: "ERR_FSQLITE_SNAPSHOT_OWNERSHIP_UNAVAILABLE",
  });
  assert.deepEqual(
    worker.requests.map((r) => r.kind),
    ["init"],
  );
  await client.close();
});

test("rollback, prepared bindings and export remain usable under exclusive ownership", async (t) => {
  const { open } = fixture(t);
  const { db } = await open();
  await db.execute("CREATE TABLE items(id INTEGER PRIMARY KEY,v TEXT)");
  await assert.rejects(
    db.transaction(async (tx) => {
      await tx.execute("INSERT INTO items VALUES(1,?)", ["undo"]);
      throw new Error("rollback");
    }),
    /rollback/,
  );
  const statement = await db.prepare("INSERT INTO items VALUES(?,?)");
  await statement.run([2, "keep"]);
  await statement.finalize();
  assert.deepEqual((await db.query("SELECT * FROM items")).rows, [{ id: 2, v: "keep" }]);
  assert.ok((await db.export()).byteLength >= 512);
  await db.checkpoint();
  await db.close();
});
