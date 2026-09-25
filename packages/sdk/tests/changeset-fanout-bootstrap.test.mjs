// Production capture/outbox -> bootstrap/apply/order -> all-replica source ACK.
// Reference SQLite owns SQL transactions; this is not Rust/WASM qualification.
import assert from "node:assert/strict";
import { test } from "node:test";
import { DatabaseSync } from "node:sqlite";
import { createHash } from "node:crypto";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";
import * as bootstrap from "../src/changeset-bootstrap.ts";
import { ChangesetFanout } from "../src/changeset-fanout.ts";
import { ChangesetOutbox } from "../src/changeset-outbox.ts";
import { ChangesetOrder } from "../src/changeset-order.ts";
import { applyChangeset } from "../src/changeset-apply.ts";

const OUTBOX = '"__fsqlite_changeset_outbox"';
const PROGRESS = '"__fsqlite_changeset_fanout_progress"';
const ROSTER = '"__fsqlite_changeset_fanout"';
const ROOT = "device/incarnation-7:seed";
const STREAM = "device/incarnation-7";
const SCHEMA = "CREATE TABLE notes(id INTEGER PRIMARY KEY, body TEXT, data BLOB)";
const checksum = b => createHash("sha256").update(b).digest("hex");
const chunkId = i => i === 0 ? ROOT : `${ROOT}/chunk/${i}`;
const route = id => ({ receiverId: id, orderedSourceId: STREAM });
const noOp = async () => {};
const cursorWrite = sql => sql.startsWith(`UPDATE OR ABORT main.${PROGRESS}`);
const reclaimWrite = sql => sql.startsWith(`UPDATE OR ABORT main.${OUTBOX}`);
const rejected = e => typeof e?.code === "string" && /^ERR_FSQLITE_(BOOTSTRAP|FANOUT|OUTBOX)_/.test(e.code);

class SqliteTarget {
  constructor(path = ":memory:", initialize = true) {
    this.db = new DatabaseSync(path);
    this.db.exec("PRAGMA foreign_keys=ON; PRAGMA recursive_triggers=ON; PRAGMA busy_timeout=0");
    if (initialize) this.db.exec(SCHEMA);
    this.active = false;
    this.closed = false;
    this.before = noOp;
    this.after = noOp;
    this.beforeCommit = noOp;
    this.afterCommit = noOp;
    this.sql = [];
  }
  executor() {
    return {
      execute: async (sql, params = []) => {
        this.sql.push(sql);
        await this.before(sql, params);
        const changed = Number(this.db.prepare(sql).run(...params).changes);
        return (await this.after(sql, params, changed)) ?? changed;
      },
      query: async (sql, params = []) => {
        this.sql.push(sql);
        await this.before(sql, params);
        const rows = this.db.prepare(sql).all(...params).map(r => Object.values(r));
        await this.after(sql, params, rows);
        return { rowArrays: rows };
      },
    };
  }
  async transaction(work) {
    assert.equal(this.active, false, "no second transaction/commit inside source ACK");
    this.active = true;
    this.db.exec("BEGIN");
    let committed = false;
    try {
      const result = await work(this.executor());
      await this.beforeCommit();
      this.db.exec("COMMIT");
      committed = true;
      await this.afterCommit();
      return result;
    } catch (e) {
      if (!committed) this.db.exec("ROLLBACK");
      throw e;
    } finally { this.active = false; }
  }
  rows() {
    return this.db.prepare("SELECT CAST(id AS TEXT), hex(body), hex(data) FROM notes ORDER BY id")
      .all().map(r => Object.values(r));
  }
  close() { if (!this.closed) { this.db.close(); this.closed = true; } }
}

async function setup(t, { replicas = ["east", "west"], rows = 3, path, journal = "WAL", ordered = true } = {}) {
  const source = new SqliteTarget(path);
  t.after(() => source.close());
  source.db.exec(`PRAGMA journal_mode=${journal}`);
  const fanout = replicas === null ? null : await ChangesetFanout.open(source, replicas);
  for (let i = 0; i < rows; i++) {
    const id = i === rows - 1 ? 9223372036854775807n : BigInt(i + 1);
    source.db.prepare("INSERT INTO notes VALUES (?,?,?)").run(id, `seed:${i}\0🌍`, new Uint8Array([0, i & 255, 255]));
  }
  const outbox = new ChangesetOutbox(source);
  const seed = await outbox.bootstrapChunks({ deliveryId: ROOT, tables: ["notes"], chunkRows: 1 });
  const chunks = [];
  for (let i = 0; i < seed.chunks; i++) chunks.push((await outbox.read(chunkId(i))).changeset);
  const baselineRows = source.rows();
  await outbox.record(async tx => {
    await tx.execute("UPDATE notes SET body=? WHERE id=?", ["incremental", 1n]);
    await tx.execute("INSERT INTO notes VALUES (?,?,?)", [100000n, "after seed", new Uint8Array([7, 0, 255])]);
  }, { deliveryId: "device/incarnation-7:delta", tables: ["notes"] });
  const increment = await outbox.read("device/incarnation-7:delta");
  assert.equal(increment.delivery.sequence, BigInt(seed.chunks + 1));
  const installations = new Map();
  async function install(id) {
    if (installations.has(id)) return installations.get(id);
    const target = new SqliteTarget();
    t.after(() => target.close());
    const manifest = await bootstrap.createBootstrapManifest({
      receiverId: id, deliveryId: ROOT, tables: ["notes"],
      chunks: seed.chunks, changes: seed.changes, byteLength: seed.byteLength,
    }, async i => chunks[i]);
    const receiver = new bootstrap.ChangesetBootstrapReceiver(target, {
      receiverId: id, tables: ["notes"],
      ...(ordered ? { orderedSourceId: STREAM } : {}),
      confirmCommit: async () => { assert.equal(target.active, false); },
    });
    for (let i = 0; i < chunks.length; i++) await receiver.stage(manifest, i, chunks[i]);
    const receipt = await receiver.install(manifest);
    assert.deepEqual(target.rows(), baselineRows);
    const result = { target, manifest, receipt, receiver };
    installations.set(id, result);
    return result;
  }
  async function ack(id, options = {}) {
    const { manifest, receipt } = await install(id);
    return bootstrap.acknowledgeFanoutBootstrapInstall(source, manifest, receipt, {
      receiverId: id, ...(ordered ? { orderedSourceId: STREAM } : {}), ...options,
    });
  }
  async function partial(id, count) {
    await install(id);
    const member = fanout.forReplica(id);
    for (let i = 0; i < count; i++) {
      const d = (await outbox.read(chunkId(i))).delivery;
      await member.acknowledge(d.deliveryId, d.sha256);
    }
  }
  return { source, fanout, outbox, seed, chunks, increment, install, ack, partial, baselineRows };
}
function saved(source) {
  return {
    entries: source.db.prepare(`SELECT seq,delivery_id,sha256,byte_length,change_count,scope,acknowledged,hex(payload) FROM ${OUTBOX} ORDER BY seq`).all(),
    cursors: source.db.prepare(`SELECT * FROM ${PROGRESS} ORDER BY replica_id`).all(),
    roster: source.db.prepare(`SELECT * FROM ${ROSTER}`).all(),
    sequence: source.db.prepare("SELECT * FROM sqlite_sequence").all(),
  };
}
async function frontier(f, positions) {
  const progress = await f.fanout.progress();
  assert.deepEqual(progress.replicas.map(r => Number(r.sequence)), positions);
  const minimum = Math.min(...positions);
  assert.equal(progress.acknowledgedThrough, BigInt(minimum));
  assert.equal(progress.sourceSequence, f.increment.delivery.sequence);
  const rows = f.source.db.prepare(`SELECT seq,acknowledged,length(payload),byte_length FROM ${OUTBOX} ORDER BY seq`).all().map(r => Object.values(r));
  for (const [sequence, acknowledged, storedBytes, byteLength] of rows) {
    assert.equal(acknowledged, Number(sequence <= minimum));
    assert.equal(storedBytes, sequence <= minimum ? 0 : byteLength);
  }
}
async function applyIncrement(f, id) {
  const { target } = await f.install(id);
  const order = new ChangesetOrder(target, { receiverId: id, sourceId: STREAM });
  const { delivery, changeset } = f.increment;
  await order.apply({ sequence: delivery.sequence, deliveryId: delivery.deliveryId, sha256: delivery.sha256, changeset },
    (scoped, bytes) => applyChangeset(scoped, bytes, { tables: ["notes"], deliveryId: delivery.deliveryId }));
  assert.deepEqual(target.rows(), f.source.rows());
  await f.fanout.forReplica(id).acknowledge(delivery.deliveryId, delivery.sha256);
}

test("all required replicas install, then original N+1 converges without premature reclamation", async t => {
  const f = await setup(t);
  assert.equal(typeof bootstrap.acknowledgeFanoutBootstrapInstall, "function");
  const before = saved(f.source), n = f.seed.chunks;
  assert.equal(await f.ack("east"), n);
  await frontier(f, [n, 0]);
  assert.deepEqual(saved(f.source).entries, before.entries, "a fast replica must not release another member's seed");
  assert.equal((await f.fanout.forReplica("east").pending())[0].sequence, BigInt(n + 1));
  assert.equal((await f.fanout.forReplica("west").pending())[0].sequence, 1n);
  await applyIncrement(f, "east");
  await frontier(f, [n + 1, 0]);
  assert.equal(await f.ack("west"), n);
  await frontier(f, [n + 1, n]);
  assert.deepEqual((await f.outbox.read(f.increment.delivery.deliveryId)).changeset, f.increment.changeset);
  await applyIncrement(f, "west");
  await frontier(f, [n + 1, n + 1]);
  const final = saved(f.source);
  assert.equal(await f.ack("east"), 0);
  assert.equal(await f.ack("west"), 0);
  assert.deepEqual(saved(f.source), final, "historical install ACK must not rewind incremental cursors");
  assert.deepEqual(final.sequence, before.sequence);
  const oracle = new DatabaseSync(":memory:");
  t.after(() => oracle.close()); oracle.exec(SCHEMA);
  for (const bytes of [...f.chunks, f.increment.changeset]) assert.equal(oracle.applyChangeset(bytes), true);
  assert.deepEqual(oracle.prepare("SELECT CAST(id AS TEXT),hex(body),hex(data) FROM notes ORDER BY id").all().map(r => Object.values(r)), f.source.rows());
});

test("every three-member seed-prefix combination retains exactly the minimum frontier", async t => {
  // Independent real SQL setups; 27 combinations of 0,1,2 seed ACKs.
  for (let a = 0; a <= 2; a++) for (let b = 0; b <= 2; b++) for (let c = 0; c <= 2; c++) {
    const f = await setup(t, { replicas: ["a", "b", "c"], rows: 2 });
    await f.partial("a", a); await f.partial("b", b); await f.partial("c", c);
    await frontier(f, [a, b, c]);
    assert.equal(await f.ack("a"), 2 - a); await frontier(f, [2, b, c]);
    assert.equal(await f.ack("c"), 2 - c); await frontier(f, [2, b, 2]);
    assert.equal(await f.ack("b"), 2 - b); await frontier(f, [2, 2, 2]);
    for (const id of ["a", "b", "c"]) assert.equal(await f.ack(id), 0);
    f.source.close();
    for (const id of ["a", "b", "c"]) (await f.install(id)).target.close();
  }
});

test("a large seed crosses metadata pages and advances only the requested member", async t => {
  const f = await setup(t, { rows: 40, replicas: ["a", "b", "c"] });
  await f.partial("b", 17); await f.partial("c", 34);
  assert.equal(await f.ack("a"), 40); await frontier(f, [40, 17, 34]);
  assert.equal(await f.ack("b"), 23); await frontier(f, [40, 40, 34]);
  assert.equal(await f.ack("c"), 6); await frontier(f, [40, 40, 40]);
});

for (const ordered of [true, false]) test(`empty source seed still needs every member (ordered=${ordered})`, async t => {
  const f = await setup(t, { rows: 0, ordered });
  assert.equal(f.seed.chunks, 1); assert.equal(f.seed.changes, 0);
  assert.equal(await f.ack("east"), 1); await frontier(f, [1, 0]);
  assert.equal(await f.ack("west"), 1); await frontier(f, [1, 1]);
});

test("single-member fanout and legacy single-recipient ACK keep separate authority", async t => {
  const f = await setup(t, { replicas: ["east"] });
  const { manifest, receipt } = await f.install("east");
  const before = saved(f.source);
  await assert.rejects(bootstrap.acknowledgeBootstrapInstall(f.source, manifest, receipt, route("east")), rejected);
  assert.deepEqual(saved(f.source), before);
  assert.equal(await f.ack("east"), f.seed.chunks); await frontier(f, [f.seed.chunks]);
  const g = await setup(t, { replicas: null });
  const install = await g.install("east");
  await assert.rejects(bootstrap.acknowledgeFanoutBootstrapInstall(g.source, install.manifest, install.receipt, route("east")), rejected);
  assert.equal(await bootstrap.acknowledgeBootstrapInstall(g.source, install.manifest, install.receipt, route("east")), g.seed.chunks);
  assert.equal(await bootstrap.acknowledgeBootstrapInstall(g.source, install.manifest, install.receipt, route("east")), 0);
  assert.deepEqual((await g.outbox.read(g.increment.delivery.deliveryId)).changeset, g.increment.changeset);
});

test("an authentic install for a nonmember does not change any required member", async t => {
  const f = await setup(t), before = saved(f.source);
  const { manifest, receipt } = await f.install("outsider");
  await assert.rejects(bootstrap.acknowledgeFanoutBootstrapInstall(f.source, manifest, receipt, route("outsider")), rejected);
  assert.deepEqual(saved(f.source), before);
});

test("forged, stripped, unconfirmed and wrong-route install receipts cannot release source bytes", async t => {
  const f = await setup(t), { manifest, receipt } = await f.install("east"), before = saved(f.source);
  const variants = [
    { ...receipt, confirmed: false }, { ...receipt, installed: false },
    { ...receipt, receiverId: "west" }, { ...receipt, deliveryId: "other" },
    { ...receipt, sha256: "0".repeat(64) }, { ...receipt, chunks: receipt.chunks + 1 },
    { ...receipt, changes: receipt.changes + 1 }, { ...receipt, byteLength: receipt.byteLength + 1 },
    { ...receipt, replayed: "true" }, { ...receipt, order: undefined },
    { ...receipt, order: { ...receipt.order, streamId: "restored-other-source" } },
    { ...receipt, order: { ...receipt.order, sequence: `0${receipt.chunks}` } },
    { ...receipt, order: { ...receipt.order, sequence: receipt.chunks } },
    { ...receipt, order: { ...receipt.order, protocol: "legacy" } },
    Object.create(receipt),
  ];
  for (const wrong of variants) {
    await assert.rejects(bootstrap.acknowledgeFanoutBootstrapInstall(f.source, manifest, wrong, route("east")), rejected);
    assert.deepEqual(saved(f.source), before);
  }
  await assert.rejects(bootstrap.acknowledgeFanoutBootstrapInstall(f.source, manifest, receipt, route("west")), rejected);
  await assert.rejects(bootstrap.acknowledgeFanoutBootstrapInstall(f.source, manifest, receipt, { receiverId: "east" }), rejected);
  let calls = 0;
  const getter = { ...receipt };
  Object.defineProperty(getter, "confirmed", { get() { calls++; return true; } });
  await assert.rejects(bootstrap.acknowledgeFanoutBootstrapInstall(f.source, manifest, getter, route("east")), rejected);
  assert.equal(calls, 0);
});

test("changed source totals/scope and corrupt bodies fail even for already-advanced replicas", async t => {
  for (const mutate of [
    db => db.exec(`UPDATE ${OUTBOX} SET payload=zeroblob(byte_length) WHERE seq=2`),
    db => db.exec(`UPDATE ${OUTBOX} SET sha256='${"0".repeat(64)}' WHERE seq=2`),
    db => db.exec(`UPDATE ${OUTBOX} SET byte_length=byte_length+1 WHERE seq=2`),
    db => db.exec(`UPDATE ${OUTBOX} SET change_count=change_count+1 WHERE seq=2`),
    db => db.exec(`DELETE FROM ${OUTBOX} WHERE seq=2`),
    db => db.exec(`DELETE FROM ${OUTBOX} WHERE seq=4`),
    db => db.exec(`UPDATE ${PROGRESS} SET delivery_id='missing' WHERE replica_id='east'`),
    db => db.exec(`DELETE FROM ${PROGRESS} WHERE replica_id='west'`),
    db => db.exec(`UPDATE ${ROSTER} SET roster='{"version":1,"replicas":["east"]}'`),
  ]) {
    const f = await setup(t); await f.ack("east");
    mutate(f.source.db);
    const before = saved(f.source);
    await assert.rejects(f.ack("east"), rejected);
    assert.deepEqual(saved(f.source), before);
  }
});

for (const pick of [cursorWrite, reclaimWrite]) {
  test(`write errors and false affected-row results roll back both states (${pick.name})`, async t => {
    for (const response of [0, 2, new Error("injected IO")]) {
      const f = await setup(t); await f.ack("east"); await f.install("west");
      const before = saved(f.source);
      f.source.after = async sql => {
        if (pick(sql)) { if (response instanceof Error) throw response; return response; }
      };
      await assert.rejects(f.ack("west"));
      f.source.after = noOp;
      assert.deepEqual(saved(f.source), before);
      assert.equal(await f.ack("west"), f.seed.chunks);
    }
  });
  test(`cancellation waits for admitted mutation and rolls back (${pick.name})`, async t => {
    const f = await setup(t); await f.ack("east"); await f.install("west");
    const before = saved(f.source), control = new AbortController();
    let reached, release;
    const admitted = new Promise(r => { reached = r; }), resume = new Promise(r => { release = r; });
    f.source.after = async sql => { if (pick(sql)) { reached(); await resume; } };
    let settled = false;
    const task = f.ack("west", { signal: control.signal });
    const failed = assert.rejects(task, e => e.code === "ERR_FSQLITE_BOOTSTRAP_CANCELLED");
    task.then(() => { settled = true; }, () => { settled = true; });
    await admitted; control.abort(); await new Promise(r => setTimeout(r, 5));
    assert.equal(settled, false); assert.equal(f.source.active, true);
    release(); await failed; f.source.after = noOp;
    assert.deepEqual(saved(f.source), before);
  });
}

test("pre-aborted and expired operations publish no cursor or payload change", async t => {
  const f = await setup(t); await f.install("east"); const before = saved(f.source);
  const control = new AbortController(); control.abort();
  const sqlCount = f.source.sql.length;
  await assert.rejects(f.ack("east", { signal: control.signal }), e => e.code === "ERR_FSQLITE_BOOTSTRAP_CANCELLED");
  assert.equal(f.source.sql.length, sqlCount);
  f.source.before = async () => { await new Promise(r => setTimeout(r, 10)); };
  await assert.rejects(f.ack("east", { timeoutMs: 2 }), e => e.code === "ERR_FSQLITE_BOOTSTRAP_TIMEOUT");
  f.source.before = noOp;
  assert.deepEqual(saved(f.source), before);
});

test("deferred foreign-key COMMIT failure rolls back cursor and reclamation", async t => {
  const f = await setup(t); await f.ack("east"); await f.install("west");
  f.source.db.exec("CREATE TABLE parent(id INTEGER PRIMARY KEY); CREATE TABLE child(id INTEGER REFERENCES parent DEFERRABLE INITIALLY DEFERRED)");
  const before = saved(f.source);
  f.source.beforeCommit = async () => { f.source.db.exec("INSERT INTO child VALUES(1)"); };
  await assert.rejects(f.ack("west"), /FOREIGN KEY/);
  f.source.beforeCommit = noOp;
  assert.deepEqual(saved(f.source), before);
  assert.equal(await f.ack("west"), f.seed.chunks);
});

test("outer rollback also rolls back an otherwise successful scoped fanout ACK", async t => {
  const f = await setup(t); await f.ack("east"); const i = await f.install("west"), before = saved(f.source);
  await assert.rejects(f.source.transaction(async tx => {
    assert.equal(await bootstrap.acknowledgeFanoutBootstrapInstall({ transaction: work => work(tx) }, i.manifest, i.receipt, route("west")), f.seed.chunks);
    throw new Error("outer rollback");
  }), /outer rollback/);
  assert.deepEqual(saved(f.source), before);
});

test("lost source COMMIT response is replayed without repeating cursor advancement", async t => {
  const f = await setup(t); await f.ack("east"); await f.install("west");
  f.source.afterCommit = async () => { throw new Error("lost response after commit"); };
  await assert.rejects(f.ack("west"), /lost response/);
  f.source.afterCommit = noOp;
  await frontier(f, [f.seed.chunks, f.seed.chunks]);
  assert.equal(await f.ack("west"), 0);
  assert.deepEqual((await f.outbox.read(f.increment.delivery.deliveryId)).changeset, f.increment.changeset);
});

test("input route, manifest and receipt are captured before SQL admission yields", async t => {
  const f = await setup(t), i = await f.install("east");
  const manifest = structuredClone(i.manifest), receipt = structuredClone(i.receipt), options = route("east");
  f.source.before = async () => {
    f.source.before = noOp;
    manifest.tables[0] = "wrong"; manifest.receiverId = "west";
    receipt.order.streamId = "wrong"; receipt.confirmed = false;
    options.receiverId = "west"; options.orderedSourceId = "wrong";
  };
  assert.equal(await bootstrap.acknowledgeFanoutBootstrapInstall(f.source, manifest, receipt, options), f.seed.chunks);
  await frontier(f, [f.seed.chunks, 0]);
});

test("source callbacks still cannot edit fanout progress and raw ACK cannot bypass a member", async t => {
  const f = await setup(t), before = saved(f.source);
  const first = (await f.outbox.read(ROOT)).delivery;
  await assert.rejects(f.outbox.acknowledge(ROOT, first.sha256), rejected);
  await assert.rejects(f.outbox.record(async tx => {
    await tx.execute(`UPDATE ${PROGRESS} SET sequence=1,delivery_id=?,sha256=? WHERE replica_id='west'`, [ROOT, first.sha256]);
    await tx.execute("INSERT INTO notes VALUES(50000,'must roll back',X'')");
  }, { tables: ["notes"], deliveryId: "bad-work" }), rejected);
  assert.deepEqual(saved(f.source), before);
});

test("two file-backed source owners conflict and retry without publishing mixed progress", { timeout: 15000 }, async t => {
  const path = join(mkdtempSync(join(tmpdir(), "fsqlite-fanout-overlap-")), "source.db");
  const f = await setup(t, { path, replicas: ["east", "north", "west"] });
  await f.ack("east"); await f.install("west"); const north = await f.install("north");
  const second = new SqliteTarget(path, false); t.after(() => second.close());
  const before = saved(second);
  let reached, release;
  const entered = new Promise(r => { reached = r; }), resume = new Promise(r => { release = r; });
  f.source.after = async (sql, params) => {
    if (cursorWrite(sql) && params[3] === "west") { reached(); await resume; }
  };
  const pending = f.ack("west");
  try {
    await entered;
    assert.deepEqual(saved(second), before, "observer sees neither provisional cursor nor reclaimed bytes");
    await assert.rejects(bootstrap.acknowledgeFanoutBootstrapInstall(second,
      north.manifest, north.receipt, route("north")), /locked|busy/i);
  } finally { release(); }
  assert.equal(await pending, f.seed.chunks); f.source.after = noOp;
  await frontier(f, [f.seed.chunks, 0, f.seed.chunks]);
  assert.equal(await bootstrap.acknowledgeFanoutBootstrapInstall(second,
    north.manifest, north.receipt, route("north")), f.seed.chunks);
  await frontier(f, [f.seed.chunks, f.seed.chunks, f.seed.chunks]);
});

test("missing membership tables and metadata triggers cannot be bypassed by a bulk ACK", async t => {
  for (const sql of [
    `DROP TABLE ${PROGRESS}`, `DROP TABLE ${ROSTER}`,
    `CREATE TRIGGER bad AFTER UPDATE ON ${PROGRESS} BEGIN INSERT INTO notes VALUES(98765,'forbidden',X''); END`,
    `CREATE TEMP TRIGGER bad AFTER UPDATE ON main.${PROGRESS} BEGIN INSERT INTO notes VALUES(98765,'forbidden',X''); END`,
    `INSERT INTO ${PROGRESS} VALUES('intruder',0,'','')`,
  ]) {
    const f = await setup(t); await f.install("east");
    const rows = f.source.rows(), entries = saved(f.source).entries;
    f.source.db.exec(sql);
    await assert.rejects(f.ack("east"), rejected);
    assert.deepEqual(f.source.rows(), rows);
    assert.deepEqual(f.source.db.prepare(`SELECT seq,delivery_id,sha256,byte_length,change_count,scope,acknowledged,hex(payload) FROM ${OUTBOX} ORDER BY seq`).all(), entries);
  }
});

test("explicit seed forgetting ends install-ACK replay; it never reconstructs lost history", async t => {
  const f = await setup(t); await f.ack("east"); await f.ack("west");
  await assert.rejects(f.fanout.forgetBootstrapChunks(ROOT, f.seed.sha256), rejected);
  await applyIncrement(f, "east"); await applyIncrement(f, "west");
  assert.equal(await f.fanout.forgetBootstrapChunks(ROOT, f.seed.sha256), true);
  const before = saved(f.source);
  await assert.rejects(f.ack("east"), rejected);
  assert.deepEqual(saved(f.source), before);
});

for (const journal of ["WAL", "DELETE"]) for (const phase of ["cursor", "reclaim", "before-commit", "after-commit"]) {
  test(`SIGKILL/reopen preserves atomic fanout ACK (${journal}/${phase})`, async t => {
    const path = join(mkdtempSync(join(tmpdir(), "fsqlite-fanout-bootstrap-")), "source.db");
    const f = await setup(t, { path, journal });
    await f.ack("east"); const i = await f.install("west");
    f.source.close();
    const module = new URL("../src/changeset-bootstrap.ts", import.meta.url).href;
    const code = `
      import { DatabaseSync } from 'node:sqlite';
      import { acknowledgeFanoutBootstrapInstall } from ${JSON.stringify(module)};
      const db = new DatabaseSync(${JSON.stringify(path)});
      db.exec('PRAGMA foreign_keys=ON');
      const phase = ${JSON.stringify(phase)};
      const crash = () => process.kill(process.pid,'SIGKILL');
      const tx = {
        execute: async (sql,p=[]) => {
          const n = Number(db.prepare(sql).run(...p).changes);
          if ((phase==='cursor' && sql.startsWith('UPDATE OR ABORT main.\"__fsqlite_changeset_fanout_progress\"')) ||
              (phase==='reclaim' && sql.startsWith('UPDATE OR ABORT main.\"__fsqlite_changeset_outbox\"'))) crash();
          return n;
        },
        query: async (sql,p=[]) => ({rowArrays:db.prepare(sql).all(...p).map(r=>Object.values(r))}),
      };
      const source = { transaction: async work => {
        db.exec('BEGIN'); const result = await work(tx);
        if(phase==='before-commit') crash(); db.exec('COMMIT');
        if(phase==='after-commit') crash(); return result;
      }};
      await acknowledgeFanoutBootstrapInstall(source, ${JSON.stringify(i.manifest)}, ${JSON.stringify(i.receipt)}, ${JSON.stringify(route("west"))});
      process.exitCode=9;
    `;
    const child = spawnSync(process.execPath, ["--experimental-transform-types",
      "--experimental-loader", new URL("./helpers/fanout-source-loader.mjs", import.meta.url).href,
      "--input-type=module", "-e", code], { encoding: "utf8", timeout: 15000 });
    assert.equal(child.signal, "SIGKILL", child.stderr);
    const reopened = new SqliteTarget(path, false); t.after(() => reopened.close());
    const group = await ChangesetFanout.open(reopened, ["west", "east"]);
    const n = f.seed.chunks, committed = phase === "after-commit";
    await frontier({ ...f, source: reopened, fanout: group }, [n, committed ? n : 0]);
    assert.equal(await bootstrap.acknowledgeFanoutBootstrapInstall(reopened, i.manifest, i.receipt, route("west")), committed ? 0 : n);
    await frontier({ ...f, source: reopened, fanout: group }, [n, n]);
    const pending = await new ChangesetOutbox(reopened).read(f.increment.delivery.deliveryId);
    assert.equal(checksum(pending.changeset), f.increment.delivery.sha256);
    assert.equal(reopened.db.prepare("PRAGMA integrity_check").get().integrity_check, "ok");
  });
}
