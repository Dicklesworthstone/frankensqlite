// Production SDK + WorkerConnectionHost + snapshot stores. SQL runs against
// Node SQLite and browser storage uses deterministic models, NOT WASM/OPFS.
// node --experimental-loader=./packages/sdk/tests/helpers/source-loader.mjs \
//   --test packages/sdk/tests/opfs-persistence.test.mjs
import assert from "node:assert/strict";
import { test } from "node:test";
import { installIndexedDbModel } from "../../worker/tests/helpers/indexeddb-model.mjs";
import { deferred, installOpfsModel } from "../../worker/tests/helpers/opfs-model.mjs";
import { sqliteSnapshotWorker } from "../../worker/tests/helpers/snapshot-sqlite-core.mjs";
import { FrankenDB } from "../src/database.ts";
import { FrankenCheckpointCommitError, FrankenDBQueue } from "../src/queue.ts";
import { FrankenWorkerClient } from "../src/worker-client.ts";

function fixture(t) {
  const model = installOpfsModel();
  const previousIdb = Object.getOwnPropertyDescriptor(globalThis, "indexedDB");
  const previousStore = Object.getOwnPropertyDescriptor(globalThis, "IDBObjectStore");
  const indexeddb = installIndexedDbModel();
  const connections = [];
  t.after(async () => {
    // Close through the host even when an invalid ready response made the SDK
    // dispose its transport. A test conduit is not an actual browser worker.
    for (const connection of connections) {
      await connection.host.handle({ kind: "close", requestId: 0 });
      connection.worker.terminate();
    }
    model.restore();
    for (const [key, descriptor] of [
      ["indexedDB", previousIdb],
      ["IDBObjectStore", previousStore],
    ]) {
      if (descriptor) Object.defineProperty(globalThis, key, descriptor);
      else Reflect.deleteProperty(globalThis, key);
    }
  });
  function connection(hooks) {
    const value = sqliteSnapshotWorker(hooks);
    connections.push(value);
    return value;
  }
  async function open(name = "documents", options = {}, hooks) {
    const value = connection(hooks);
    return {
      ...value,
      db: await FrankenDB.open({
        dbName: name,
        persistence: "opfs-snapshot",
        ...options,
        worker: value.worker,
      }),
    };
  }
  async function queue(name = "documents", options = {}, hooks) {
    const value = connection(hooks);
    return {
      ...value,
      db: await FrankenDBQueue.open(
        {
          dbName: name,
          persistence: "opfs-snapshot",
          worker: value.worker,
        },
        { checkpointOnCommit: true, ...options },
      ),
    };
  }
  return { model, indexeddb, connection, open, queue };
}

const schema = "CREATE TABLE documents(id INTEGER PRIMARY KEY, body TEXT NOT NULL, data BLOB)";
const rows = (db) => db.query("SELECT id, body, data FROM documents ORDER BY id");
const sqlCounts = (events) => events.filter((sql) => sql.startsWith("INSERT")).length;

test("OPFS public database opens, publishes and restores real SQLite bytes", async (t) => {
  const { model, open } = fixture(t);
  const first = await open("folder/name 🚀", { resultEncoding: "binary" });
  assert.equal(first.db.persistence, "opfs-snapshot");
  assert.equal(first.db.path, "folder/name 🚀");
  assert.equal(first.db.snapshotRevision, null);
  assert.equal(first.db.checkpointRecoverySupported, true);
  await first.db.execute(schema);
  await first.db.execute("INSERT INTO documents VALUES (?, ?, ?)", [
    1,
    "saved 🚀",
    new Uint8Array([0, 1, 255]),
  ]);
  const receipt = await first.db.checkpoint();
  assert.equal(receipt.parentRevision, null);
  assert.equal(first.db.snapshotRevision, receipt.revision);
  assert.equal(model.counts.publications, 1);
  await first.db.close();
  const reopened = await open("folder/name 🚀");
  assert.equal(reopened.db.snapshotRevision, receipt.revision);
  assert.deepEqual(reopened.counts(), { creates: 0, imports: 1 });
  assert.deepEqual((await rows(reopened.db)).rowArrays, [
    [1, "saved 🚀", new Uint8Array([0, 1, 255])],
  ]);
  assert.deepEqual((await reopened.db.query("PRAGMA integrity_check")).rowArrays, [["ok"]]);
});

test("OPFS queued transactions acknowledge only their published commit", async (t) => {
  const { model, queue, open } = fixture(t);
  const first = await queue();
  assert.equal(first.db.checkpointOnCommit, true);
  await first.db.transaction(async (tx) => {
    await tx.execute(schema);
    await tx.execute("INSERT INTO documents VALUES (1, ?, NULL)", ["first"]);
  });
  const revision = first.db.snapshotRevision;
  assert.equal(model.counts.publications, 1);
  const failed = new Error("rollback the entire callback");
  await assert.rejects(
    first.db.transaction(async (tx) => {
      await tx.execute("INSERT INTO documents VALUES (2, ?, NULL)", ["not saved"]);
      throw failed;
    }),
    (error) => error === failed,
  );
  assert.equal(model.counts.publications, 1);
  await first.db.close();
  const reopened = await open();
  assert.equal(reopened.db.snapshotRevision, revision);
  assert.deepEqual((await rows(reopened.db)).rowArrays, [[1, "first", null]]);
});

test("OPFS queue holds later SQL and close behind an in-flight publication", async (t) => {
  const { model, queue } = fixture(t);
  const first = await queue();
  await first.db.transaction((tx) => tx.execute(schema));
  const entered = deferred(),
    release = deferred();
  model.hooks.beforeClose = async () => {
    entered.resolve();
    await release.promise;
  };
  let accepted = false,
    later = false,
    closed = false;
  const write = first.db
    .transaction((tx) => tx.execute("INSERT INTO documents VALUES (1, ?, NULL)", ["first"]))
    .then(() => {
      accepted = true;
    });
  await entered.promise;
  const following = first.db.transaction(async (tx) => {
    later = true;
    await tx.execute("INSERT INTO documents VALUES (2, ?, NULL)", ["second"]);
  });
  const close = first.db.close().then(() => {
    closed = true;
  });
  assert.equal(accepted, false);
  assert.equal(later, false);
  assert.equal(closed, false);
  assert.equal(first.worker.terminateCount, 0);
  model.hooks.beforeClose = undefined;
  release.resolve();
  await Promise.all([write, following, close]);
  assert.equal(model.counts.publications, 3);
  assert.equal(first.worker.terminateCount, 1);
});

test("OPFS store close acknowledgement loss is confirmed without replay or export", async (t) => {
  const { model, queue, open } = fixture(t);
  const first = await queue();
  await first.db.transaction((tx) => tx.execute(schema));
  const parent = first.db.snapshotRevision;
  let callbacks = 0;
  model.hooks.afterClose = () => {
    throw new Error("published, acknowledgement lost");
  };
  await assert.rejects(
    first.db.transaction(async (tx) => {
      callbacks++;
      await tx.execute("INSERT INTO documents VALUES (1, ?, NULL)", ["committed"]);
      return 73;
    }),
    (error) =>
      error instanceof FrankenCheckpointCommitError && error.value === 73 && error.sqlCommitted,
  );
  assert.equal(first.db.snapshotRevision, parent);
  assert.equal(first.db.stats.checkpointRecoveryRequired, true);
  await assert.rejects(
    first.db.transaction(() => {
      callbacks++;
    }),
    { code: "ERR_FSQLITE_CHECKPOINT_RECOVERY_REQUIRED" },
  );
  const exports = first.events.filter((event) => event === "export").length;
  const inserts = sqlCounts(first.events);
  const publications = model.counts.publications;
  model.hooks.afterClose = undefined;
  const recovered = await first.db.recoverCheckpoint();
  assert.equal(recovered.parentRevision, parent);
  assert.equal(first.db.snapshotRevision, recovered.revision);
  assert.equal(first.db.stats.checkpointRecoveryRequired, false);
  assert.equal(callbacks, 1);
  assert.equal(sqlCounts(first.events), inserts);
  assert.equal(first.events.filter((event) => event === "export").length, exports);
  assert.equal(model.counts.publications, publications);
  await first.db.transaction((tx) =>
    tx.execute("INSERT INTO documents VALUES (2, ?, NULL)", ["next"]),
  );
  await first.db.close();
  const reopened = await open();
  assert.deepEqual((await rows(reopened.db)).rowArrays, [
    [1, "committed", null],
    [2, "next", null],
  ]);
});

test("OPFS corrupt worker receipt can be recovered by reading authoritative bytes", async (t) => {
  const { connection } = fixture(t);
  const value = connection();
  const handle = value.host.handle.bind(value.host);
  let corrupt = false;
  value.host.handle = async (request) => {
    const response = await handle(request);
    if (corrupt && request.kind === "checkpoint" && response.kind === "checkpoint-result") {
      return { ...response, data: { ...response.data, sha256: "bad" } };
    }
    return response;
  };
  const db = await FrankenDB.open({
    dbName: "documents",
    persistence: "opfs-snapshot",
    worker: value.worker,
  });
  await db.execute(schema);
  corrupt = true;
  await assert.rejects(db.checkpoint(), { code: "ERR_FSQLITE_SNAPSHOT_RECEIPT" });
  const requests = value.worker.requests.length;
  await assert.rejects(db.checkpoint(), { code: "ERR_FSQLITE_SNAPSHOT_RECEIPT" });
  assert.equal(value.worker.requests.length, requests);
  corrupt = false;
  const receipt = await db.recoverCheckpoint();
  assert.equal(receipt.revision, db.snapshotRevision);
  await db.checkpoint();
});

test("OPFS failed staging preserves saved data and queue recovery never replays SQL", async (t) => {
  const { model, queue, open } = fixture(t);
  const first = await queue();
  await first.db.transaction((tx) => tx.execute(schema));
  const parent = first.db.snapshotRevision;
  let callbacks = 0;
  model.hooks.write = () => {
    throw new DOMException("full", "QuotaExceededError");
  };
  await assert.rejects(
    first.db.transaction(async (tx) => {
      callbacks++;
      await tx.execute("INSERT INTO documents VALUES (1, ?, NULL)", ["pending"]);
    }),
    FrankenCheckpointCommitError,
  );
  assert.equal(first.db.snapshotRevision, parent);
  assert.equal(model.counts.publications, 1);
  const reader = await open();
  assert.deepEqual((await rows(reader.db)).rowArrays, []);
  await reader.db.close();
  await assert.rejects(first.db.recoverCheckpoint(), {
    code: "ERR_FSQLITE_SNAPSHOT_NOT_CONFIRMED",
  });
  assert.equal(first.db.stats.checkpointRecoveryRequired, true);
  model.hooks.write = undefined;
  const saved = await first.db.checkpoint();
  assert.equal(saved.parentRevision, parent);
  assert.equal(callbacks, 1);
  assert.equal(sqlCounts(first.events), 1);
  await first.db.close();
  const reopened = await open();
  assert.deepEqual((await rows(reopened.db)).rowArrays, [[1, "pending", null]]);
});

test("independent OPFS sessions reject stale publication and keep local SQL exportable", async (t) => {
  const { open, queue } = fixture(t);
  const seed = await open();
  await seed.db.execute(schema);
  await seed.db.checkpoint();
  await seed.db.close();
  const winner = await queue(),
    stale = await queue();
  await winner.db.transaction((tx) =>
    tx.execute("INSERT INTO documents VALUES (1, ?, NULL)", ["winner"]),
  );
  await assert.rejects(
    stale.db.transaction((tx) =>
      tx.execute("INSERT INTO documents VALUES (2, ?, NULL)", ["local"]),
    ),
    (error) =>
      error instanceof FrankenCheckpointCommitError &&
      error.cause.code === "ERR_FSQLITE_SNAPSHOT_CONFLICT",
  );
  await assert.rejects(stale.db.recoverCheckpoint(), {
    code: "ERR_FSQLITE_SNAPSHOT_NOT_CONFIRMED",
  });
  const image = await stale.db.export();
  assert.ok(image.byteLength >= 512);
  assert.equal(stale.db.stats.checkpointRecoveryRequired, true);
  await assert.rejects(stale.db.close(), FrankenCheckpointCommitError);
  await winner.db.close();
  const reopened = await open();
  assert.deepEqual((await rows(reopened.db)).rowArrays, [[1, "winner", null]]);
});

test("OPFS export/import supports new names but refuses replacing existing storage", async (t) => {
  const { open } = fixture(t);
  const source = await open("source");
  await source.db.execute(schema);
  await source.db.execute("INSERT INTO documents VALUES (1, ?, NULL)", ["copied"]);
  const bytes = await source.db.export();
  const target = await open("target", { snapshot: bytes });
  assert.equal(target.db.snapshotRevision, null);
  assert.deepEqual((await rows(target.db)).rowArrays, [[1, "copied", null]]);
  await target.db.checkpoint();
  await target.db.close();
  await assert.rejects(open("target", { snapshot: await source.db.export() }), {
    code: "ERR_FSQLITE_SNAPSHOT_EXISTS",
  });
});

test("OPFS named namespaces isolate unrelated databases", async (t) => {
  const { open } = fixture(t);
  const one = await open("one"),
    two = await open("two");
  for (const [value, body] of [
    [one, "first"],
    [two, "second"],
  ]) {
    await value.db.execute(schema);
    await value.db.execute("INSERT INTO documents VALUES (1, ?, NULL)", [body]);
    await value.db.checkpoint();
    await value.db.close();
  }
  const first = await open("one"),
    second = await open("two");
  assert.notEqual(first.db.snapshotRevision, second.db.snapshotRevision);
  assert.deepEqual((await rows(first.db)).rowArrays, [[1, "first", null]]);
  assert.deepEqual((await rows(second.db)).rowArrays, [[1, "second", null]]);
});

test("OPFS publication refuses active manual and managed transactions", async (t) => {
  const { model, open } = fixture(t);
  const { db } = await open();
  await db.execute(schema);
  await db.executeBatch("BEGIN; INSERT INTO documents VALUES (1, 'uncommitted', NULL)");
  await assert.rejects(db.checkpoint(), { code: "ERR_FSQLITE_SNAPSHOT_TRANSACTION" });
  assert.equal(model.counts.publications, 0);
  await db.executeBatch("ROLLBACK");
  await db.transaction(async (tx) => {
    await tx.execute("INSERT INTO documents VALUES (2, ?, NULL)", ["committed"]);
    await assert.rejects(db.checkpoint(), { code: "ERR_FSQLITE_TRANSACTION_OWNERSHIP" });
  });
  await db.checkpoint();
  assert.deepEqual((await rows(db)).rowArrays, [[2, "committed", null]]);
});

test("OPFS unavailable APIs never silently fall back to volatile memory", async (t) => {
  const { connection } = fixture(t);
  Object.defineProperty(globalThis, "navigator", { configurable: true, value: {} });
  const value = connection();
  await assert.rejects(
    FrankenDB.open({ dbName: "documents", persistence: "opfs-snapshot", worker: value.worker }),
    { code: "ERR_FSQLITE_SNAPSHOT_UNAVAILABLE" },
  );
  assert.deepEqual(value.counts(), { creates: 0, imports: 0 });
  assert.equal(value.worker.terminateCount, 1);
});

test("unsupported page-VFS modes still fail before creating a core database", async (t) => {
  const { connection } = fixture(t);
  for (const persistence of ["opfs", "indexeddb"]) {
    const value = connection();
    await assert.rejects(
      FrankenDB.open({ dbName: "documents", persistence, worker: value.worker }),
      { code: "ERR_FSQLITE_UNSUPPORTED_PERSISTENCE" },
    );
    assert.deepEqual(value.counts(), { creates: 0, imports: 0 });
  }
});

test("memory import retains its core-assigned path without inventing persistence", async (t) => {
  const { connection, open } = fixture(t);
  const source = await open();
  await source.db.execute(schema);
  const target = connection();
  const db = await FrankenDB.import(await source.db.export(), {
    dbName: "display-name",
    persistence: "memory",
    worker: target.worker,
  });
  assert.equal(db.path, target.handles[0].path);
  assert.equal(db.persistence, "memory");
  assert.equal(db.checkpointRecoverySupported, false);
  await assert.rejects(db.checkpoint(), { code: "ERR_FSQLITE_SNAPSHOT_MODE" });
  await db.close();
});

function peer(reply) {
  const listeners = new Map();
  const worker = {
    requests: [],
    terminated: 0,
    addEventListener(kind, listener) {
      (listeners.get(kind) ?? listeners.set(kind, new Set()).get(kind)).add(listener);
    },
    removeEventListener(kind, listener) {
      listeners.get(kind)?.delete(listener);
    },
    postMessage(request) {
      this.requests.push(request);
      const data =
        request.kind === "init"
          ? { kind: "ready", requestId: request.requestId, data: reply(request.config) }
          : { kind: "close-result", requestId: request.requestId };
      for (const listener of listeners.get("message") ?? []) listener({ data });
    },
    terminate() {
      this.terminated++;
    },
  };
  return worker;
}

for (const [requested, accepted] of [
  ["opfs-snapshot", "memory"],
  ["opfs-snapshot", "indexeddb-snapshot"],
  ["indexeddb-snapshot", "memory"],
  ["indexeddb-snapshot", "opfs-snapshot"],
  ["memory", "opfs-snapshot"],
]) {
  test(`init rejects ${requested} being acknowledged as ${accepted}`, async () => {
    const worker = peer((config) => ({ path: config.dbName, persistence: accepted }));
    await assert.rejects(FrankenDB.open({ dbName: "documents", persistence: requested, worker }), {
      code: "ERR_FSQLITE_PERSISTENCE_POLICY",
    });
    assert.equal(worker.terminated, 1);
    assert.deepEqual(
      worker.requests.map((request) => request.kind),
      ["init"],
    );
  });
}

for (const persistence of ["opfs-snapshot", "indexeddb-snapshot"]) {
  test(`init rejects another ${persistence} namespace`, async () => {
    const worker = peer(() => ({ path: "other-database", persistence }));
    await assert.rejects(FrankenDB.open({ dbName: "documents", persistence, worker }), {
      code: "ERR_FSQLITE_PERSISTENCE_POLICY",
    });
    assert.equal(worker.terminated, 1);
  });
}

test("direct client captures caller config once and fences a mismatched acknowledgement", async () => {
  let reads = 0;
  const worker = peer(() => ({ path: "documents", persistence: "memory" }));
  const client = new FrankenWorkerClient(worker);
  await assert.rejects(
    client.init({
      dbName: "documents",
      get persistence() {
        reads++;
        return "opfs-snapshot";
      },
    }),
    { code: "ERR_FSQLITE_PERSISTENCE_POLICY" },
  );
  assert.equal(reads, 1);
  await assert.rejects(client.execute("INSERT INTO anything VALUES (1)"), {
    code: "ERR_FSQLITE_PERSISTENCE_POLICY",
  });
  assert.equal(worker.requests.length, 1);
  await client.close();
});

for (const key of [
  "path",
  "persistence",
  "snapshot",
  "checkpointRecovery",
  "preparedStatementLimits",
  "resultEncoding",
]) {
  test(`ready ${key} accessors cannot grant storage authority`, async () => {
    let reads = 0;
    const worker = peer(() =>
      Object.defineProperty({ path: "documents", persistence: "opfs-snapshot" }, key, {
        enumerable: true,
        get() {
          reads++;
          return key === "path" ? "documents" : "opfs-snapshot";
        },
      }),
    );
    await assert.rejects(
      FrankenDB.open({ dbName: "documents", persistence: "opfs-snapshot", worker }),
      { code: "ERR_FSQLITE_WORKER_RESPONSE" },
    );
    assert.equal(reads, 0);
    assert.equal(worker.terminated, 1);
  });
}

test("ready identity is copied before a custom transport can mutate it", async () => {
  const ready = {
    path: "documents",
    persistence: "opfs-snapshot",
    snapshot: null,
    checkpointRecovery: 1,
  };
  const worker = peer(() => ready);
  const post = worker.postMessage.bind(worker);
  worker.postMessage = (request) => {
    post(request);
    ready.path = "other";
    ready.persistence = "memory";
    ready.checkpointRecovery = undefined;
  };
  const db = await FrankenDB.open({ dbName: "documents", persistence: "opfs-snapshot", worker });
  assert.equal(db.path, "documents");
  assert.equal(db.persistence, "opfs-snapshot");
  assert.equal(db.checkpointRecoverySupported, true);
  await db.close();
});

// A fatal delivery notice after store publication forces the public SDK to
// retire its transport. Reopen uses a NEW host and actual imported image.
async function lostPublication(t, persistence, queue = false, publish = true) {
  const setup = fixture(t);
  const value = setup.connection();
  const options = { dbName: "restart", persistence, worker: value.worker };
  const db = queue
    ? await FrankenDBQueue.open(options, { checkpointOnCommit: true })
    : await FrankenDB.open(options);
  if (queue) await db.transaction((tx) => tx.execute(schema));
  else {
    await db.execute(schema);
    await db.checkpoint();
  }
  const parent = db.snapshotRevision;
  const handle = value.host.handle.bind(value.host);
  value.host.handle = async (request) => {
    if (request.kind !== "checkpoint") return handle(request);
    if (publish) {
      const response = await handle(request);
      assert.equal(response.kind, "checkpoint-result");
    }
    return {
      kind: "worker-fatal",
      error: { code: "ERR_FSQLITE_WORKER_TRANSPORT", message: "Publication delivery lost" },
    };
  };
  if (queue) {
    await assert.rejects(
      db.transaction((tx) => tx.execute("INSERT INTO documents VALUES (1, ?, NULL)", ["restored"])),
      FrankenCheckpointCommitError,
    );
  } else {
    await db.execute("INSERT INTO documents VALUES (1, ?, NULL)", ["restored"]);
    await assert.rejects(db.checkpoint(), { code: "ERR_FSQLITE_WORKER_TRANSPORT" });
  }
  const ticket = db.pendingCheckpointRecovery;
  assert.ok(ticket);
  assert.equal(Object.isFrozen(ticket), true);
  assert.deepEqual(
    { path: ticket.path, persistence: ticket.persistence, parent: ticket.parentRevision },
    { path: "restart", persistence, parent },
  );
  assert.notEqual(ticket.publicationId, parent);
  assert.equal(value.worker.terminateCount, 1);
  assert.equal(value.events.filter((sql) => sql === "COMMIT").length, queue ? 2 : 0);
  return { ...setup, ...value, db, ticket };
}

for (const persistence of ["opfs-snapshot", "indexeddb-snapshot"]) {
  test(`${persistence} restores an exact failed publication after retiring the original worker`, async (t) => {
    const setup = await lostPublication(t, persistence);
    const { ticket } = setup;
    // A JSON round trip represents an application retaining this small identity.
    const copied = JSON.parse(JSON.stringify(ticket));
    const opened = await setup.open("restart", { persistence, requireCheckpoint: copied });
    assert.deepEqual(opened.counts(), { creates: 0, imports: 1 });
    assert.equal(opened.db.snapshotRevision, ticket.publicationId);
    assert.equal(opened.db.pendingCheckpointRecovery, null);
    assert.deepEqual((await rows(opened.db)).rowArrays, [[1, "restored", null]]);
    assert.equal(opened.events.includes("export"), false);
    assert.equal(sqlCounts(opened.events), 0);
    assert.equal(
      opened.worker.requests.some((request) => request.kind === "checkpoint"),
      false,
    );
    // The captured token was an SDK precondition, not an unrecognized wire option.
    assert.equal(Object.hasOwn(opened.worker.requests[0].config, "requireCheckpoint"), false);
    await opened.db.execute("INSERT INTO documents VALUES (2, ?, NULL)", ["next"]);
    const next = await opened.db.checkpoint();
    assert.equal(next.parentRevision, ticket.publicationId);
    await opened.db.close();
    const reader = await setup.open("restart", { persistence });
    assert.deepEqual((await rows(reader.db)).rowArrays, [
      [1, "restored", null],
      [2, "next", null],
    ]);
  });

  test(`${persistence} queued recovery ticket remains available after failed close`, async (t) => {
    const setup = await lostPublication(t, persistence, true);
    await assert.rejects(setup.db.close(), FrankenCheckpointCommitError);
    assert.deepEqual(setup.db.pendingCheckpointRecovery, setup.ticket);
    const value = setup.connection();
    const opened = await FrankenDBQueue.open(
      { dbName: "restart", persistence, requireCheckpoint: setup.ticket, worker: value.worker },
      { checkpointOnCommit: true },
    );
    assert.equal(opened.snapshotRevision, setup.ticket.publicationId);
    assert.equal(opened.stats.checkpointRecoveryRequired, false);
    assert.equal(opened.pendingCheckpointRecovery, null);
    await opened.transaction((tx) =>
      tx.execute("INSERT INTO documents VALUES (2, ?, NULL)", ["next"]),
    );
    await opened.close();
    const reader = await setup.open("restart", { persistence });
    assert.deepEqual((await rows(reader.db)).rowArrays, [
      [1, "restored", null],
      [2, "next", null],
    ]);
  });

  test(`${persistence} requireCheckpoint refuses a prior image when publication never arrived`, async (t) => {
    const setup = await lostPublication(t, persistence, false, false);
    await assert.rejects(setup.open("restart", { persistence, requireCheckpoint: setup.ticket }), {
      code: "ERR_FSQLITE_SNAPSHOT_NOT_CONFIRMED",
    });
    const reader = await setup.open("restart", { persistence });
    assert.equal(reader.db.snapshotRevision, setup.ticket.parentRevision);
    assert.deepEqual((await rows(reader.db)).rowArrays, []);
  });

  test(`${persistence} requireCheckpoint refuses a superseded publication without claiming it never committed`, async (t) => {
    const setup = await lostPublication(t, persistence);
    const peer = await setup.open("restart", { persistence });
    await peer.db.execute("INSERT INTO documents VALUES (2, ?, NULL)", ["later"]);
    const latest = await peer.db.checkpoint();
    await peer.db.close();
    await assert.rejects(
      setup.open("restart", { persistence, requireCheckpoint: setup.ticket }),
      (error) =>
        error.code === "ERR_FSQLITE_SNAPSHOT_NOT_CONFIRMED" &&
        error.suggestion.includes("does not prove"),
    );
    const reader = await setup.open("restart", { persistence });
    assert.equal(reader.db.snapshotRevision, latest.revision);
    assert.deepEqual((await rows(reader.db)).rowArrays, [
      [1, "restored", null],
      [2, "later", null],
    ]);
  });

  test(`${persistence} requireCheckpoint refuses absent storage and incorrect parent lineage`, async (t) => {
    const setup = await lostPublication(t, persistence);
    await assert.rejects(
      setup.open("restart", {
        persistence,
        requireCheckpoint: { ...setup.ticket, parentRevision: crypto.randomUUID() },
      }),
      { code: "ERR_FSQLITE_SNAPSHOT_NOT_CONFIRMED" },
    );
    await assert.rejects(
      setup.open("missing", {
        persistence,
        requireCheckpoint: { ...setup.ticket, path: "missing" },
      }),
      { code: "ERR_FSQLITE_SNAPSHOT_NOT_CONFIRMED" },
    );
  });

  test(`${persistence} requireCheckpoint verifies stored bytes rather than trusting a revision alone`, async (t) => {
    const setup = await lostPublication(t, persistence);
    if (persistence === "opfs-snapshot") {
      const [key, bytes] = [...setup.model.files].find(([key]) => key.endsWith("/head"));
      const changed = bytes.slice();
      changed[changed.length - 1] ^= 1;
      setup.model.files.set(key, changed);
    } else {
      const record = setup.indexeddb.databases
        .get("frankensqlite:snapshot:v1:restart")
        .values.get("head");
      new Uint8Array(record.bytes)[record.byteLength - 1] ^= 1;
    }
    await assert.rejects(setup.open("restart", { persistence, requireCheckpoint: setup.ticket }), {
      code: "ERR_FSQLITE_SNAPSHOT_CORRUPT",
    });
  });
}

test("checkpoint recovery identities validate before worker allocation or input transfer", async () => {
  const valid = {
    path: "restart",
    persistence: "opfs-snapshot",
    publicationId: crypto.randomUUID(),
    parentRevision: null,
  };
  let creates = 0;
  const worker = () => {
    creates++;
    throw new Error("must not allocate");
  };
  for (const requireCheckpoint of [
    null,
    [],
    {},
    { ...valid, path: "elsewhere" },
    { ...valid, persistence: "indexeddb-snapshot" },
    { ...valid, publicationId: "bad" },
    { ...valid, parentRevision: valid.publicationId },
    { ...valid, parentRevision: undefined },
    { ...valid, parentRevision: "bad" },
  ]) {
    await assert.rejects(
      FrankenDB.open({
        dbName: "restart",
        persistence: "opfs-snapshot",
        requireCheckpoint,
        worker,
      }),
      { code: "ERR_FSQLITE_SNAPSHOT_INPUT" },
    );
  }
  const image = new Uint8Array(512);
  await assert.rejects(
    FrankenDB.open({
      dbName: "restart",
      persistence: "opfs-snapshot",
      requireCheckpoint: valid,
      snapshot: image,
      worker,
    }),
    { code: "ERR_FSQLITE_SNAPSHOT_INPUT" },
  );
  await assert.rejects(
    FrankenDB.open({ dbName: "restart", persistence: "memory", requireCheckpoint: valid, worker }),
    { code: "ERR_FSQLITE_SNAPSHOT_INPUT" },
  );
  assert.equal(creates, 0);
  assert.equal(image.byteLength, 512);
});

test("required checkpoint input is captured before an asynchronous open can mutate it", async (t) => {
  const setup = await lostPublication(t, "opfs-snapshot");
  const ticket = { ...setup.ticket };
  const entered = deferred(),
    release = deferred();
  const value = setup.connection({
    beforeImport: async () => {
      entered.resolve();
      await release.promise;
    },
  });
  const opening = FrankenDB.open({
    dbName: "restart",
    persistence: "opfs-snapshot",
    requireCheckpoint: ticket,
    worker: value.worker,
  });
  await entered.promise;
  ticket.publicationId = crypto.randomUUID();
  ticket.parentRevision = null;
  release.resolve();
  const opened = await opening;
  assert.equal(opened.snapshotRevision, setup.ticket.publicationId);
  await opened.close();
});

test("recovery identity is absent during pending publication and clears after same-session confirmation", async (t) => {
  const { model, open } = fixture(t);
  const { db } = await open();
  assert.equal(db.pendingCheckpointRecovery, null);
  await db.execute(schema);
  const entered = deferred(),
    release = deferred();
  model.hooks.afterClose = async () => {
    entered.resolve();
    await release.promise;
    throw new Error("lost");
  };
  const publishing = db.checkpoint();
  void publishing.catch(() => {});
  await entered.promise;
  assert.equal(db.pendingCheckpointRecovery, null);
  release.resolve();
  await assert.rejects(publishing);
  const ticket = db.pendingCheckpointRecovery;
  assert.ok(ticket);
  assert.equal(ticket.parentRevision, null);
  model.hooks.afterClose = undefined;
  const saved = await db.recoverCheckpoint();
  assert.equal(ticket.publicationId, saved.revision);
  assert.equal(db.pendingCheckpointRecovery, null);
});

for (const persistence of ["opfs-snapshot", "indexeddb-snapshot"]) {
  test(`${persistence} required initial checkpoint cannot be silently ignored`, async (t) => {
    const setup = fixture(t);
    const value = setup.connection();
    const requireCheckpoint = {
      path: "never-published",
      persistence,
      publicationId: crypto.randomUUID(),
      parentRevision: null,
    };
    await assert.rejects(
      FrankenDB.open({
        dbName: "never-published",
        persistence,
        requireCheckpoint,
        worker: value.worker,
      }),
      { code: "ERR_FSQLITE_SNAPSHOT_NOT_CONFIRMED" },
    );
    assert.equal(value.worker.terminateCount, 1);
    assert.equal(value.events.includes("export"), false);
    assert.equal(sqlCounts(value.events), 0);
  });
}
