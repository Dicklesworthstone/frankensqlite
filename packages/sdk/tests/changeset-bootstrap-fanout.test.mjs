// Production bootstrap/application/order/fanout/storage on reference SQLite.
// Native Session generates bytes; the transaction owner is a reference adapter.
import assert from "node:assert/strict";
import { test } from "node:test";
import { DatabaseSync } from "node:sqlite";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import {
  acknowledgeBootstrapInstall, acknowledgeFanoutBootstrapInstall, ChangesetBootstrapReceiver, createBootstrapManifest,
  readBootstrapManifest,
} from "../src/changeset-bootstrap.ts";
import {
  ChangesetFanout, CHANGESET_FANOUT_TABLE, CHANGESET_FANOUT_PROGRESS_TABLE,
  assertSingleRecipient,
} from "../src/changeset-fanout.ts";
import { ChangesetOrder } from "../src/changeset-order.ts";
import { applyChangeset } from "../src/changeset-apply.ts";
import {
  CHANGESET_OUTBOX_TABLE, acknowledgeDelivery, chunkId, ensure, find, load, store,
} from "../src/changeset-outbox-store.ts";

const OUTBOX = `"${CHANGESET_OUTBOX_TABLE}"`;
const PROGRESS = `"${CHANGESET_FANOUT_PROGRESS_TABLE}"`;
const ROSTER = `"${CHANGESET_FANOUT_TABLE}"`;
const rootId = "source:seed/1", streamId = "source:incarnation/1";
const schema = "CREATE TABLE IF NOT EXISTS notes(id INTEGER PRIMARY KEY, body TEXT, data BLOB)";
class SqlTarget {
  constructor(path = ":memory:") {
    this.db = new DatabaseSync(path);
    this.db.exec(`PRAGMA foreign_keys=ON; PRAGMA recursive_triggers=ON; ${schema}`);
    this.active = false;
    this.sql = [];
    this.after = null;
    this.beforeCommit = null;
    this.afterCommit = null;
  }
  executor() {
    return {
      execute: async (sql, params = []) => {
        this.sql.push(sql);
        const result = Number(this.db.prepare(sql).run(...params).changes);
        return (await this.after?.(sql, params, result)) ?? result;
      },
      query: async (sql, params = []) => {
        this.sql.push(sql);
        const statement = this.db.prepare(sql);
        statement.setReadBigInts(true);
        const rows = statement.all(...params).map(row => Object.values(row));
        await this.after?.(sql, params, rows);
        return { rowArrays: rows };
      },
    };
  }
  async transaction(work) {
    assert.equal(this.active, false, "reference owner does not permit overlapping transactions");
    this.active = true;
    this.db.exec("BEGIN");
    let committed = false;
    try {
      const result = await work(this.executor());
      await this.beforeCommit?.();
      this.db.exec("COMMIT");
      committed = true;
      await this.afterCommit?.();
      return result;
    } catch (error) {
      if (!committed) this.db.exec("ROLLBACK");
      throw error;
    } finally { this.active = false; }
  }
  close() { this.db.close(); }
}
function rows(target) {
  return target.db.prepare("SELECT CAST(id AS TEXT),hex(body),hex(data) FROM notes ORDER BY id")
    .all().map(Object.values);
}
function snapshot(source) {
  return {
    progress: source.db.prepare(`SELECT replica_id,CAST(sequence AS TEXT),delivery_id,sha256 FROM ${PROGRESS} ORDER BY replica_id`).all().map(Object.values),
    outbox: source.db.prepare(`SELECT seq,delivery_id,sha256,byte_length,change_count,scope,acknowledged,hex(payload) FROM ${OUTBOX} ORDER BY seq`).all().map(Object.values),
    sequence: source.db.prepare("SELECT seq FROM sqlite_sequence WHERE name=?").get(CHANGESET_OUTBOX_TABLE)?.seq,
  };
}
function isCursor(sql) { return sql.startsWith(`UPDATE OR ABORT main.${PROGRESS} SET sequence=`); }
function isReclaim(sql) { return sql.startsWith(`UPDATE OR ABORT main.${OUTBOX} SET acknowledged=1,payload=X'' WHERE seq>`); }
const errorCode = suffix => error => error?.code?.endsWith(suffix) === true;

async function setup(t, { members = ["east", "west"], chunks = 3, path = ":memory:", ordered = true, fanoutMode = true } = {}) {
  const source = new SqlTarget(path);
  t.after(() => source.close());
  const fanout = fanoutMode ? await ChangesetFanout.open(source, members) : null;
  if (!fanoutMode) await source.transaction(tx => ensure(tx, true));
  const chunkBytes = [];
  // Real Session-generated rows and the production outbox serializer commit
  // together. This fixture replaces capture planning, not storage/ACK behavior.
  await source.transaction(async tx => {
    for (let i = 0; i < chunks; i++) {
      const session = source.db.createSession({ table: "notes" });
      source.db.prepare("INSERT INTO notes VALUES (?,?,?)").run(i + 1, `row\0🌍${i}`, new Uint8Array([i, 255]));
      chunkBytes.push(new Uint8Array(session.changeset()));
      session.close();
    }
    const summary = { chunks, changes: chunks, byteLength: chunkBytes.reduce((n, b) => n + b.length, 0) };
    for (let i = 0; i < chunks; i++) {
      await store(tx, chunkId(rootId, i), JSON.stringify({ tables: ["notes"], indirect: false, snapshot: true,
        stream: i === 0 ? { id: rootId, index: i, summary } : { id: rootId, index: i } }),
      { changeset: chunkBytes[i], changes: 1 }, () => {});
    }
  });
  const read = id => source.transaction(async tx => {
    const entry = await find(tx, id);
    return entry === null ? null : { delivery: entry.delivery, changeset: await load(tx, entry) };
  });
  const manifests = new Map(), receipts = new Map(), receivers = new Map();
  for (const receiverId of members) {
    const manifest = await createBootstrapManifest({ receiverId, deliveryId: rootId, tables: ["notes"], chunks,
      changes: chunks, byteLength: chunkBytes.reduce((n, b) => n + b.length, 0) },
    async i => (await read(chunkId(rootId, i))).changeset);
    manifests.set(receiverId, manifest);
    const target = new SqlTarget(); t.after(() => target.close());
    const receiver = new ChangesetBootstrapReceiver(target, { receiverId, tables: ["notes"],
      ...(ordered ? { orderedSourceId: streamId } : {}),
      confirmCommit: async () => { assert.equal(target.active, false); },
    });
    for (let i = 0; i < chunks; i++) await receiver.stage(manifest, i, chunkBytes[i]);
    receipts.set(receiverId, await receiver.install(manifest));
    assert.deepEqual(rows(target), rows(source));
    receivers.set(receiverId, { target, receiver });
  }
  const acknowledge = fanoutMode ? acknowledgeFanoutBootstrapInstall : acknowledgeBootstrapInstall;
  const ack = (receiverId, options = {}) => acknowledge(source, manifests.get(receiverId), receipts.get(receiverId), {
    receiverId, ...(ordered ? { orderedSourceId: streamId } : {}), ...options,
  });
  async function increment() {
    return source.transaction(async tx => {
      const session = source.db.createSession({ table: "notes" });
      source.db.exec("UPDATE notes SET body='increment' WHERE id=1");
      const bytes = new Uint8Array(session.changeset()); session.close();
      const delivery = await store(tx, "source:increment/1", JSON.stringify({ tables: ["notes"], indirect: false }),
        { changeset: bytes, changes: 1 }, () => {});
      return { delivery, changeset: bytes };
    });
  }
  return { source, fanout, chunks, manifests, receipts, receivers, ack, read, increment };
}

// Run the actual ACK in a separate process and die at a named SQL/COMMIT
// boundary. Parent probes reopen a new connection; no in-process rollback hook
// can clean up on behalf of the killed source.
if (process.env.FSQLITE_BOOTSTRAP_FANOUT_KILL !== undefined) {
  const { path, phase, manifest, receipt } = JSON.parse(process.env.FSQLITE_BOOTSTRAP_FANOUT_KILL);
  const source = new SqlTarget(path);
  const die = () => process.kill(process.pid, "SIGKILL");
  source.after = async sql => {
    if ((phase === "cursor" && isCursor(sql)) || (phase === "reclamation" && isReclaim(sql))) die();
  };
  if (phase === "before-commit") source.beforeCommit = async () => die();
  if (phase === "after-commit") source.afterCommit = async () => die();
  await acknowledgeFanoutBootstrapInstall(source, manifest, receipt,
    { receiverId: "west", orderedSourceId: streamId });
  throw Error("The requested process-death boundary was never reached");
}

test("one complete replica ACK retains all bytes; last replica reclaims the seed atomically", async t => {
  const f = await setup(t);
  const next = await f.increment();
  const before = snapshot(f.source);
  f.source.sql = [];
  assert.equal(await f.ack("east"), f.chunks);
  assert.equal((await f.fanout.progress()).acknowledgedThrough, 0n);
  assert.deepEqual(snapshot(f.source).outbox, before.outbox);
  assert.equal(f.source.sql.filter(isCursor).length, 1);
  assert.equal(f.source.sql.filter(isReclaim).length, 0);
  assert.deepEqual((await f.fanout.forReplica("east").pending()).map(x => x.sequence), [BigInt(f.chunks + 1)]);
  assert.equal((await f.fanout.forReplica("west").pending()).length, f.chunks + 1);
  f.source.sql = [];
  assert.equal(await f.ack("west"), f.chunks);
  assert.equal(f.source.sql.filter(isCursor).length, 1);
  assert.equal(f.source.sql.filter(isReclaim).length, 1);
  assert.equal((await f.fanout.progress()).acknowledgedThrough, BigInt(f.chunks));
  const after = snapshot(f.source);
  assert.equal(after.sequence, before.sequence);
  assert.equal(after.outbox.length, before.outbox.length);
  assert.ok(after.outbox.slice(0, f.chunks).every(row => row[6] === 1 && row[7] === ""));
  assert.deepEqual(after.outbox.at(-1), before.outbox.at(-1));
  for (const id of ["east", "west"]) {
    const { target } = f.receivers.get(id);
    const order = new ChangesetOrder(target, { receiverId: id, sourceId: streamId });
    await order.apply({ ...next.delivery, changeset: next.changeset },
      (owned, bytes) => applyChangeset(owned, bytes, { tables: ["notes"], deliveryId: next.delivery.deliveryId }));
    assert.deepEqual(rows(target), rows(f.source));
    await f.fanout.forReplica(id).acknowledge(next.delivery.deliveryId, next.delivery.sha256);
  }
  assert.equal((await f.fanout.progress()).acknowledgedThrough, BigInt(f.chunks + 1));
});

test("replayed seed ACK never rewinds a replica already beyond the seed", async t => {
  const f = await setup(t);
  const next = await f.increment();
  await f.ack("east");
  await f.fanout.forReplica("east").acknowledge(next.delivery.deliveryId, next.delivery.sha256);
  const before = snapshot(f.source);
  assert.equal(await f.ack("east"), 0);
  assert.deepEqual(snapshot(f.source), before);
  await f.ack("west");
  assert.equal((await f.fanout.progress()).acknowledgedThrough, BigInt(f.chunks));
  assert.deepEqual((await f.read(next.delivery.deliveryId)).changeset, next.changeset);
  const after = snapshot(f.source);
  assert.equal(await f.ack("east"), 0);
  assert.equal(await f.ack("west"), 0);
  assert.deepEqual(snapshot(f.source), after);
});

test("a complete ACK advances a partial receiver, reclaiming only the all-member prefix", async t => {
  const f = await setup(t, { members: ["east", "west", "north"], chunks: 5 });
  for (const id of ["west", "north"]) {
    const first = await f.read(rootId);
    await f.fanout.forReplica(id).acknowledge(rootId, first.delivery.sha256);
  }
  await f.ack("east");
  assert.equal((await f.fanout.progress()).acknowledgedThrough, 1n);
  assert.equal((await f.read(rootId)).changeset, null);
  for (let i = 1; i < f.chunks; i++) assert.ok((await f.read(chunkId(rootId, i))).changeset.length > 0);
  assert.equal(await f.ack("west"), 4);
  assert.equal((await f.fanout.progress()).acknowledgedThrough, 1n);
  assert.equal(await f.ack("north"), 4);
  assert.equal((await f.fanout.progress()).acknowledgedThrough, 5n);
});

for (const members of [["only"], ["east", "west"], ["éast🌍", "west", "WEST"]]) {
  test(`complete seed receipt works for ${JSON.stringify(members)}`, async t => {
    const f = await setup(t, { members, ordered: false, chunks: 1 });
    for (const id of members) assert.equal(await f.ack(id), 1);
    assert.equal((await f.fanout.progress()).acknowledgedThrough, 1n);
  });
}

test("default single-recipient policy still rejects fanout, and raw ACK bypass stays blocked", async t => {
  const f = await setup(t);
  const before = snapshot(f.source);
  await assert.rejects(acknowledgeBootstrapInstall(f.source, f.manifests.get("east"), f.receipts.get("east"),
    { receiverId: "east", orderedSourceId: streamId }), errorCode("FANOUT_STATE"));
  await assert.rejects(f.source.transaction(tx => assertSingleRecipient(tx)), errorCode("FANOUT_STATE"));
  assert.deepEqual(snapshot(f.source), before);
});

test("fanout route cannot silently fall back after both authority tables disappear", async t => {
  const f = await setup(t);
  f.source.db.exec(`DROP TABLE ${PROGRESS}; DROP TABLE ${ROSTER}`);
  const before = f.source.db.prepare(`SELECT seq,hex(payload),acknowledged FROM ${OUTBOX}`).all();
  await assert.rejects(f.ack("east"), errorCode("FANOUT_STATE"));
  assert.deepEqual(f.source.db.prepare(`SELECT seq,hex(payload),acknowledged FROM ${OUTBOX}`).all(), before);
});

for (const receiverId of [null, 1, "", {}, []]) {
  test(`invalid receiver route ${JSON.stringify(receiverId)} rejects before SQL`, async t => {
    const f = await setup(t); f.source.sql = [];
    await assert.rejects(f.ack("east", { receiverId }), errorCode("BOOTSTRAP_INPUT"));
    assert.equal(f.source.sql.length, 0);
  });
}

for (const mutate of [
  r => ({ ...r, receiverId: "west" }),
  r => ({ ...r, sha256: "0".repeat(64) }),
  r => ({ ...r, confirmed: false }),
  r => ({ ...r, installed: false }),
  r => ({ ...r, chunks: r.chunks - 1 }),
  r => ({ ...r, order: { ...r.order, streamId: "other" } }),
  r => ({ ...r, order: { ...r.order, sequence: "2" } }),
  r => { const { order, ...stripped } = r; return stripped; },
]) {
  test(`forged complete ACK is not fanout authority: ${mutate.toString()}`, async t => {
    const f = await setup(t), before = snapshot(f.source);
    f.receipts.set("east", mutate(f.receipts.get("east")));
    await assert.rejects(f.ack("east"));
    assert.deepEqual(snapshot(f.source), before);
  });
}

test("a correctly hashed foreign receiver is still not a fanout member", async t => {
  const f = await setup(t, { members: ["east", "west", "foreign"] });
  // A coherent roster replacement is trusted DB-writer territory, but the
  // route must still belong to the CURRENT retained roster before mutation.
  f.source.db.exec(`DELETE FROM ${PROGRESS} WHERE replica_id='foreign'`);
  f.source.db.prepare(`UPDATE ${ROSTER} SET roster=?`).run(JSON.stringify({ version: 1, replicas: ["east", "west"] }));
  const before = snapshot(f.source);
  await assert.rejects(f.ack("foreign"), errorCode("FANOUT_ACK"));
  assert.deepEqual(snapshot(f.source), before);
});

for (const sql of [
  `DELETE FROM ${PROGRESS} WHERE replica_id='west'`,
  `UPDATE ${PROGRESS} SET sequence=2 WHERE replica_id='west'`,
  `UPDATE ${OUTBOX} SET payload=zeroblob(byte_length) WHERE seq=2`,
  `DELETE FROM ${OUTBOX} WHERE seq=2`,
  `UPDATE ${OUTBOX} SET acknowledged=1,payload=X'' WHERE seq=2`,
]) {
  test(`damaged authority or retained seed stops without changes: ${sql}`, async t => {
    const f = await setup(t); f.source.db.exec(sql);
    const before = snapshot(f.source);
    await assert.rejects(f.ack("east"));
    assert.deepEqual(snapshot(f.source), before);
  });
}

for (const phase of ["cursor", "reclamation"]) {
  for (const fault of ["throw", "wrong-count", "cancel"]) {
    test(`${fault} after ${phase} rolls back both cursor and all-replica reclamation`, async t => {
      const f = await setup(t); await f.ack("east");
      const before = snapshot(f.source), controller = new AbortController();
      let hit = false;
      f.source.after = async (sql, params, result) => {
        if (!(phase === "cursor" ? isCursor(sql) : isReclaim(sql))) return;
        hit = true;
        if (fault === "throw") throw Error("injected storage failure");
        if (fault === "wrong-count") return result + 1;
        controller.abort("cancel after write");
      };
      await assert.rejects(f.ack("west", { signal: controller.signal }));
      f.source.after = null;
      assert.equal(hit, true);
      assert.deepEqual(snapshot(f.source), before);
      assert.equal(await f.ack("west"), f.chunks);
    });
  }
}

test("a real deferred COMMIT failure rolls back cursor, reclamation and constraint side effects", async t => {
  const f = await setup(t); await f.ack("east");
  f.source.db.exec("CREATE TABLE p(id INTEGER PRIMARY KEY); CREATE TABLE debt(id INTEGER REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)");
  const before = snapshot(f.source);
  f.source.beforeCommit = async () => f.source.db.exec("INSERT INTO debt VALUES(42)");
  await assert.rejects(f.ack("west"), /FOREIGN KEY/);
  f.source.beforeCommit = null;
  assert.deepEqual(snapshot(f.source), before);
  assert.equal(f.source.db.prepare("SELECT count(*) AS n FROM debt").get().n, 0);
  assert.equal(await f.ack("west"), f.chunks);
});

test("loss after source COMMIT retries without repeating cursor or payload writes", async t => {
  const f = await setup(t); await f.ack("east");
  f.source.afterCommit = async () => { throw Error("lost COMMIT acknowledgement"); };
  await assert.rejects(f.ack("west"), /lost COMMIT/);
  f.source.afterCommit = null;
  assert.equal((await f.fanout.progress()).acknowledgedThrough, BigInt(f.chunks));
  const before = snapshot(f.source); f.source.sql = [];
  assert.equal(await f.ack("west"), 0);
  assert.equal(f.source.sql.filter(sql => isCursor(sql) || isReclaim(sql)).length, 0);
  assert.deepEqual(snapshot(f.source), before);
});

test("file reopen resumes independent cursors and finishes the original installed baseline", async t => {
  const path = join(mkdtempSync(join(tmpdir(), "fsqlite-fanout-bootstrap-")), "source.db");
  const f = await setup(t, { path }); await f.ack("east");
  const reopened = new SqlTarget(path); t.after(() => reopened.close());
  const fanout = await ChangesetFanout.open(reopened, ["west", "east"]);
  assert.equal((await fanout.progress()).acknowledgedThrough, 0n);
  assert.equal(await acknowledgeFanoutBootstrapInstall(reopened, f.manifests.get("west"), f.receipts.get("west"),
    { receiverId: "west", orderedSourceId: streamId }), f.chunks);
  assert.equal((await fanout.progress()).acknowledgedThrough, BigInt(f.chunks));
});

test("cancellation drains a suspended cursor write before releasing ownership", async t => {
  const f = await setup(t); await f.ack("east");
  const before = snapshot(f.source), controller = new AbortController();
  let resume, reached;
  const barrier = new Promise(resolve => { reached = resolve; });
  const hold = new Promise(resolve => { resume = resolve; });
  f.source.after = async sql => { if (isCursor(sql)) { reached(); await hold; } };
  let settled = false;
  const pending = f.ack("west", { signal: controller.signal });
  pending.then(() => { settled = true; }, () => { settled = true; });
  await barrier; controller.abort();
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(settled, false); assert.equal(f.source.active, true);
  resume(); await assert.rejects(pending, errorCode("BOOTSTRAP_CANCELLED"));
  f.source.after = null;
  assert.deepEqual(snapshot(f.source), before); assert.equal(f.source.active, false);
});

const route = receiverId => ({ receiverId, deliveryId: rootId, tables: ["notes"] });
test("retained-source builder reproduces the original wire manifest before and after reclamation", async t => {
  const f = await setup(t);
  for (const id of ["east", "west"]) {
    const before = snapshot(f.source); f.source.sql = [];
    const rebuilt = await readBootstrapManifest(f.source, route(id));
    assert.deepEqual(rebuilt, f.manifests.get(id));
    assert.equal(JSON.stringify(rebuilt), JSON.stringify(f.manifests.get(id)));
    assert.ok(Object.isFrozen(rebuilt)); assert.ok(Object.isFrozen(rebuilt.tables));
    assert.deepEqual(snapshot(f.source), before);
    assert.ok(!f.source.sql.some(sql => /^(UPDATE|DELETE|INSERT|CREATE)/.test(sql)));
  }
  await f.ack("east"); await f.ack("west");
  assert.equal((await f.read(rootId)).changeset, null);
  assert.deepEqual(await readBootstrapManifest(f.source, route("east")), f.manifests.get("east"));
  assert.deepEqual(await readBootstrapManifest(f.source, route("west")), f.manifests.get("west"));
  assert.equal(await acknowledgeFanoutBootstrapInstall(f.source, await readBootstrapManifest(f.source, route("west")), f.receipts.get("west"),
    { receiverId: "west", orderedSourceId: streamId }), 0);
});

test("manifest recovery never resnapshots current rows or later incremental payloads", async t => {
  const f = await setup(t); const next = await f.increment();
  f.source.db.exec("DELETE FROM notes");
  const before = snapshot(f.source); f.source.sql = [];
  assert.deepEqual(await readBootstrapManifest(f.source, route("east")), f.manifests.get("east"));
  assert.deepEqual(snapshot(f.source), before);
  assert.deepEqual((await f.read(next.delivery.deliveryId)).changeset, next.changeset);
  assert.ok(!f.source.sql.some(sql => /FROM (?:main\.)?"?notes\b/i.test(sql)));
});

test("manifest recovery accepts a partial reclaimed prefix and verifies the remaining bodies", async t => {
  const f = await setup(t);
  const first = await f.read(rootId);
  for (const id of ["east", "west"]) await f.fanout.forReplica(id).acknowledge(rootId, first.delivery.sha256);
  assert.equal((await f.read(rootId)).changeset, null);
  assert.deepEqual(await readBootstrapManifest(f.source, route("east")), f.manifests.get("east"));
  f.source.db.exec(`UPDATE ${OUTBOX} SET payload=zeroblob(byte_length) WHERE seq=2`);
  await assert.rejects(readBootstrapManifest(f.source, route("east")));
});

test("manifest route and table order are captured before asynchronous SQL admission", async t => {
  const f = await setup(t), input = route("east");
  let once = false;
  f.source.after = async () => {
    if (once) return;
    once = true; input.receiverId = "west"; input.deliveryId = "changed"; input.tables[0] = "other";
  };
  const result = await readBootstrapManifest(f.source, input);
  f.source.after = null;
  assert.deepEqual(result, f.manifests.get("east"));
});

for (const changed of [
  { receiverId: "" }, { deliveryId: "" }, { receiverId: "\0" }, { deliveryId: "x".repeat(481) },
  { tables: [] }, { tables: ["notes", "NOTES"] }, { tables: ["__fsqlite_bootstrap_state"] },
]) {
  test(`invalid manifest route rejects without reading SQL: ${JSON.stringify(changed)}`, async t => {
    const f = await setup(t); f.source.sql = [];
    await assert.rejects(readBootstrapManifest(f.source, { ...route("east"), ...changed }), errorCode("BOOTSTRAP_INPUT"));
    assert.equal(f.source.sql.length, 0);
  });
}

test("unknown and forgotten source seeds are errors, not empty manifests or implicit reseeds", async t => {
  const source = new SqlTarget(); t.after(() => source.close());
  const before = source.db.prepare("SELECT name FROM sqlite_schema ORDER BY name").all();
  await assert.rejects(readBootstrapManifest(source, route("east")), errorCode("BOOTSTRAP_STATE"));
  assert.deepEqual(source.db.prepare("SELECT name FROM sqlite_schema ORDER BY name").all(), before);
  await source.transaction(tx => ensure(tx, true));
  await assert.rejects(readBootstrapManifest(source, route("east")), errorCode("BOOTSTRAP_STATE"));
  const f = await setup(t); const next = await f.increment();
  await f.ack("east"); await f.ack("west");
  for (const id of ["east", "west"]) await f.fanout.forReplica(id).acknowledge(next.delivery.deliveryId, next.delivery.sha256);
  const seedDigest = (await f.read(rootId)).delivery.sha256;
  await f.fanout.forgetBootstrapChunks(rootId, seedDigest);
  await assert.rejects(readBootstrapManifest(f.source, route("east")), errorCode("BOOTSTRAP_STATE"));
});

test("manifest recovery requires the exact retained source table scope", async t => {
  const f = await setup(t);
  const before = snapshot(f.source);
  await assert.rejects(readBootstrapManifest(f.source, { ...route("east"), tables: ["other"] }), errorCode("BOOTSTRAP_STATE"));
  assert.deepEqual(snapshot(f.source), before);
});

test("empty baseline retains a one-chunk manifest and advances the all-replica sequence", async t => {
  const source = new SqlTarget(); t.after(() => source.close());
  const fanout = await ChangesetFanout.open(source, ["east", "west"]);
  const bytes = new Uint8Array();
  await source.transaction(tx => store(tx, rootId, JSON.stringify({ tables: ["notes"], indirect: false, snapshot: true,
    stream: { id: rootId, index: 0, summary: { chunks: 1, changes: 0, byteLength: 0 } } }),
    { changeset: bytes, changes: 0 }, () => {}));
  for (const receiverId of ["east", "west"]) {
    const manifest = await readBootstrapManifest(source, route(receiverId));
    assert.deepEqual(manifest, await createBootstrapManifest({ ...route(receiverId), chunks: 1, changes: 0, byteLength: 0 }, async () => bytes));
    const target = new SqlTarget(); t.after(() => target.close());
    const receiver = new ChangesetBootstrapReceiver(target, { receiverId, tables: ["notes"], orderedSourceId: streamId, confirmCommit: async () => {} });
    await receiver.stage(manifest, 0, bytes);
    const receipt = await receiver.install(manifest);
    assert.equal(await acknowledgeFanoutBootstrapInstall(source, manifest, receipt, { receiverId, orderedSourceId: streamId }), 1);
  }
  assert.equal((await fanout.progress()).acknowledgedThrough, 1n);
});

test("cancelled manifest read creates no state and starts no SQL", async t => {
  const f = await setup(t), controller = new AbortController(); controller.abort();
  f.source.sql = [];
  await assert.rejects(readBootstrapManifest(f.source, route("east"), { signal: controller.signal }), errorCode("BOOTSTRAP_CANCELLED"));
  assert.equal(f.source.sql.length, 0);
});

test("manifest read checks its deadline after an awaited source operation", async t => {
  const f = await setup(t), before = snapshot(f.source);
  f.source.after = async () => { await new Promise(resolve => setTimeout(resolve, 10)); };
  await assert.rejects(readBootstrapManifest(f.source, route("east"), { timeoutMs: 2 }), errorCode("BOOTSTRAP_TIMEOUT"));
  f.source.after = null; assert.deepEqual(snapshot(f.source), before);
});

test("manifest read does not convert a lost transaction response into successful output", async t => {
  const f = await setup(t), before = snapshot(f.source);
  f.source.afterCommit = async () => { throw Error("lost read transaction response"); };
  await assert.rejects(readBootstrapManifest(f.source, route("east")), /lost read/);
  f.source.afterCommit = null;
  assert.deepEqual(snapshot(f.source), before);
  assert.deepEqual(await readBootstrapManifest(f.source, route("east")), f.manifests.get("east"));
});

test("single-recipient ACK keeps original behavior through shared manifest verification", async t => {
  const f = await setup(t, { members: ["east"], fanoutMode: false });
  const increment = await f.increment();
  const first = await f.read(rootId);
  await f.source.transaction(tx => acknowledgeDelivery(tx, rootId, first.delivery.sha256));
  const manifest = await readBootstrapManifest(f.source, route("east"));
  assert.deepEqual(manifest, f.manifests.get("east"));
  assert.equal(await f.ack("east"), f.chunks - 1);
  assert.equal(await f.ack("east"), 0);
  assert.deepEqual(await readBootstrapManifest(f.source, route("east")), manifest);
  assert.deepEqual((await f.read(increment.delivery.deliveryId)).changeset, increment.changeset);
});

test("an explicit fanout route on a never-enrolled single source cannot reclaim anything", async t => {
  const f = await setup(t, { members: ["east"], fanoutMode: false });
  const before = f.source.db.prepare(`SELECT seq,acknowledged,hex(payload) FROM ${OUTBOX}`).all();
  await assert.rejects(acknowledgeFanoutBootstrapInstall(f.source, f.manifests.get("east"), f.receipts.get("east"),
    { receiverId: "east", orderedSourceId: streamId }), errorCode("FANOUT_STATE"));
  assert.deepEqual(f.source.db.prepare(`SELECT seq,acknowledged,hex(payload) FROM ${OUTBOX}`).all(), before);
});

for (const mode of ["WAL", "DELETE"]) {
  for (const phase of ["cursor", "reclamation", "before-commit", "after-commit"]) {
    test(`${mode}: SIGKILL at ${phase} reopens with all-or-none fanout advancement and reclamation`, async t => {
      const path = join(mkdtempSync(join(tmpdir(), "fsqlite-fanout-death-")), "source.db");
      const f = await setup(t, { path });
      f.source.db.exec(`PRAGMA journal_mode=${mode}`);
      const next = await f.increment();
      await f.ack("east");
      await f.fanout.forReplica("east").acknowledge(next.delivery.deliveryId, next.delivery.sha256);
      const before = snapshot(f.source);
      const result = spawnSync(process.execPath, [
        "--experimental-transform-types",
        `--experimental-loader=${fileURLToPath(new URL("./helpers/fanout-source-loader.mjs", import.meta.url))}`,
        fileURLToPath(import.meta.url),
      ], { env: { ...process.env, FSQLITE_BOOTSTRAP_FANOUT_KILL: JSON.stringify({ path, phase,
        manifest: f.manifests.get("west"), receipt: f.receipts.get("west") }) },
        encoding: "utf8", timeout: 10_000 });
      assert.equal(result.error, undefined, result.stderr);
      assert.equal(result.signal, "SIGKILL", result.stderr);
      const reopened = new SqlTarget(path); t.after(() => reopened.close());
      assert.deepEqual(reopened.db.prepare("PRAGMA integrity_check").all().map(Object.values), [["ok"]]);
      const fanout = await ChangesetFanout.open(reopened, ["east", "west"]);
      const committed = phase === "after-commit";
      if (!committed) assert.deepEqual(snapshot(reopened), before);
      assert.equal((await fanout.progress()).acknowledgedThrough, committed ? BigInt(f.chunks) : 0n);
      const manifest = await readBootstrapManifest(reopened, route("west"));
      assert.deepEqual(manifest, f.manifests.get("west"));
      assert.equal(await acknowledgeFanoutBootstrapInstall(reopened, manifest, f.receipts.get("west"),
        { receiverId: "west", orderedSourceId: streamId }), committed ? 0 : f.chunks);
      assert.equal((await fanout.progress()).acknowledgedThrough, BigInt(f.chunks));
      assert.deepEqual(snapshot(reopened).outbox.at(-1), before.outbox.at(-1));
      assert.equal((await fanout.progress()).replicas.find(r => r.receiverId === "east").sequence, BigInt(f.chunks + 1));
    });
  }
}

test("complete ACK preserves every small-state partial-cursor combination", async t => {
  let cases = 0;
  for (let east = 0; east <= 2; east++) for (let west = 0; west <= 2; west++) for (let north = 0; north <= 2; north++) {
    const f = await setup(t, { members: ["east", "west", "north"], chunks: 2 });
    for (const [id, count] of [["east", east], ["west", west], ["north", north]]) {
      for (let i = 0; i < count; i++) {
        const record = await f.read(chunkId(rootId, i));
        await f.fanout.forReplica(id).acknowledge(record.delivery.deliveryId, record.delivery.sha256);
      }
    }
    assert.equal(await f.ack("east"), 2 - east);
    const current = await f.fanout.progress(), minimum = Math.min(2, west, north);
    assert.equal(current.acknowledgedThrough, BigInt(minimum));
    const positions = Object.fromEntries(current.replicas.map(r => [r.receiverId, Number(r.sequence)]));
    assert.deepEqual(positions, { east: 2, north, west });
    for (let i = 0; i < 2; i++) {
      const record = await f.read(chunkId(rootId, i));
      assert.equal(record.delivery.acknowledged, i < minimum);
      assert.equal(record.changeset === null, i < minimum);
    }
    cases++;
  }
  assert.equal(cases, 27);
});

test("source manifest preserves explicit multi-table order and receiver-specific identity", async t => {
  const source = new SqlTarget(); t.after(() => source.close());
  source.db.exec("CREATE TABLE alpha(id TEXT PRIMARY KEY, value INTEGER)");
  const payloads = [];
  await source.transaction(async tx => {
    await ensure(tx, true);
    for (const [table, sql] of [["notes", "INSERT INTO notes VALUES(1,'one',X'00')"], ["alpha", "INSERT INTO alpha VALUES('a',2)"]]) {
      const session = source.db.createSession({ table });
      source.db.exec(sql); payloads.push(new Uint8Array(session.changeset())); session.close();
    }
    const summary = { chunks: 2, changes: 2, byteLength: payloads.reduce((n, b) => n + b.length, 0) };
    for (let i = 0; i < 2; i++) await store(tx, chunkId(rootId, i), JSON.stringify({ tables: ["alpha", "notes"], indirect: false,
      snapshot: true, stream: i === 0 ? { id: rootId, index: i, summary } : { id: rootId, index: i } }),
      { changeset: payloads[i], changes: 1 }, () => {});
  });
  const input = { receiverId: "east", deliveryId: rootId, tables: ["NOTES", "Alpha"] };
  const expected = await createBootstrapManifest({ ...input, chunks: 2, changes: 2,
    byteLength: payloads.reduce((n, b) => n + b.length, 0) }, async i => payloads[i]);
  assert.deepEqual(await readBootstrapManifest(source, input), expected);
  assert.notEqual((await readBootstrapManifest(source, { ...input, tables: ["alpha", "notes"] })).sha256, expected.sha256);
  assert.notEqual((await readBootstrapManifest(source, { ...input, receiverId: "west" })).sha256, expected.sha256);
  source.db.exec("ALTER TABLE notes ADD COLUMN later TEXT; DROP TABLE alpha");
  assert.deepEqual(await readBootstrapManifest(source, input), expected);
});
