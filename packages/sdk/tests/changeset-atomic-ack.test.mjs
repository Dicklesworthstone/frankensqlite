// Actual outbox/fanout/capture code on reference SQLite, not Rust-engine evidence.
import assert from "node:assert/strict";
import { test } from "node:test";
import { DatabaseSync } from "node:sqlite";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ChangesetOutbox, CHANGESET_OUTBOX_TABLE } from "../src/changeset-outbox.ts";
import { ChangesetFanout } from "../src/changeset-fanout.ts";

const T = `"${CHANGESET_OUTBOX_TABLE}"`;
const pause = () => { let resolve; const promise = new Promise(r => { resolve = r; }); return { promise, resolve }; };
const rows = (db, sql, params = []) => {
  const stmt = db.prepare(sql); stmt.setReadBigInts(true); stmt.setReturnArrays(true); return stmt.all(...params);
};
class Owner {
  constructor(path = ":memory:") {
    this.db = new DatabaseSync(path);
    this.db.exec("PRAGMA foreign_keys=ON; PRAGMA recursive_triggers=ON; CREATE TABLE IF NOT EXISTS items(id INTEGER PRIMARY KEY,value TEXT)");
    this.depth = 0; this.serial = 0; this.begins = 0; this.beforeSql = null; this.beforeCommit = null; this.afterCommit = null;
  }
  async transaction(work) {
    const depth = this.depth++, name = `ack_${++this.serial}`;
    this.begins++; this.db.exec(depth ? `SAVEPOINT ${name}` : "BEGIN");
    let committed = false, active = true;
    const check = () => assert.ok(active, "expired owner");
    const tx = {
      execute: async (sql, params = []) => { check(); await this.beforeSql?.(sql, params); return Number(this.db.prepare(sql).run(...params).changes); },
      query: async (sql, params = []) => { check(); await this.beforeSql?.(sql, params); return { rowArrays: rows(this.db, sql, params) }; },
      transaction: child => { check(); return this.transaction(child); },
    };
    try {
      const result = await work(tx); active = false; await this.beforeCommit?.(depth);
      this.db.exec(depth ? `RELEASE ${name}` : "COMMIT"); committed = true;
      await this.afterCommit?.(depth); return result;
    } catch (error) {
      active = false;
      if (!committed) this.db.exec(depth ? `ROLLBACK TO ${name}; RELEASE ${name}` : "ROLLBACK");
      throw error;
    } finally { active = false; this.depth--; }
  }
  close() { this.db.close(); }
}
async function setup(t, fanout = false, count = 3) {
  const owner = new Owner(); t.after(() => owner.close());
  const outbox = new ChangesetOutbox(owner);
  const group = fanout ? await ChangesetFanout.open(owner, ["east", "west"]) : null;
  const port = group?.forReplica("east") ?? outbox;
  const deliveries = [];
  for (let n = 1; n <= count; n++) {
    const result = await outbox.record(tx => tx.execute("INSERT INTO items VALUES(?,?)", [BigInt(n), `row-${n}`]),
      { tables: ["items"], deliveryId: `source:${n}` });
    deliveries.push(result.delivery);
  }
  const payloads = [];
  for (const entry of deliveries) payloads.push((await outbox.read(entry.deliveryId)).changeset);
  // Independently consume captured source bytes before acknowledging them.
  const replica = new DatabaseSync(":memory:");
  try {
    replica.exec("CREATE TABLE items(id INTEGER PRIMARY KEY,value TEXT)");
    for (const bytes of payloads) assert.equal(replica.applyChangeset(bytes), true);
    assert.deepEqual(rows(replica, "SELECT * FROM items ORDER BY id"), rows(owner.db, "SELECT * FROM items ORDER BY id"));
  } finally { replica.close(); }
  return { owner, outbox, group, port, deliveries, payloads };
}
const ack = entry => ({ deliveryId: entry.deliveryId, sha256: entry.sha256 });
const saved = f => rows(f.owner.db, `SELECT seq,acknowledged,length(payload) FROM ${T} ORDER BY seq`);
const positions = async f => (await f.group.progress()).replicas.map(r => r.sequence);

for (const fanout of [false, true]) {
  const mode = fanout ? "fanout" : "single";
  test(`atomic ACK ${mode}: commits once, preserves identities and exact replay`, async t => {
    const f = await setup(t, fanout), before = f.owner.begins;
    const result = await f.port.acknowledgeBatch(f.deliveries.map(ack));
    assert.equal(f.owner.begins - before, 1); assert.deepEqual(result, [true, true, true]); assert.ok(Object.isFrozen(result));
    assert.deepEqual(await f.port.acknowledgeBatch(f.deliveries.map(ack)), [false, false, false]);
    assert.equal(saved(f).length, 3, "identity tombstones must not be forgotten");
    if (fanout) {
      assert.deepEqual(await positions(f), [3n, 0n]); assert.ok(saved(f).every(r => r[1] === 0n && r[2] > 0n));
      assert.deepEqual(await f.group.forReplica("west").acknowledgeBatch(f.deliveries.map(ack)), [true, true, true]);
    }
    assert.ok(saved(f).every(r => r[1] === 1n && r[2] === 0n));
    assert.equal((await f.outbox.record(() => assert.fail("replayed source work"),
      { tables: ["items"], deliveryId: f.deliveries[0].deliveryId })).replayed, true);
  });
  test(`atomic ACK ${mode}: replay prefix and fresh suffix return individual results`, async t => {
    const f = await setup(t, fanout); await f.port.acknowledge(...Object.values(ack(f.deliveries[0])));
    assert.deepEqual(await f.port.acknowledgeBatch(f.deliveries.map(ack)), [false, true, true]);
  });
  for (const fault of ["digest", "missing", "payload"]) test(`atomic ACK ${mode}: late ${fault} restores the whole source transaction`, async t => {
    const f = await setup(t, fanout), before = saved(f);
    const input = f.deliveries.map(ack);
    if (fault === "digest") input[2].sha256 = "0".repeat(64);
    if (fault === "missing") input[2].deliveryId = "unknown";
    if (fault === "payload") f.owner.db.prepare(`UPDATE ${T} SET payload=zeroblob(length(payload)) WHERE seq=3`).run();
    await assert.rejects(f.port.acknowledgeBatch(input));
    assert.deepEqual(saved(f), before);
    if (fanout) assert.deepEqual(await positions(f), [0n, 0n]);
    if (fault === "payload") f.owner.db.prepare(`UPDATE ${T} SET payload=? WHERE seq=3`).run(f.payloads[2]);
    assert.deepEqual(await f.port.acknowledgeBatch(f.deliveries.map(ack)), [true, true, true]);
  });
  test(`atomic ACK ${mode}: capture all identities before first await`, async t => {
    const f = await setup(t, fanout), started = pause(), release = pause(); let first = true;
    f.owner.beforeSql = async () => { if (first) { first = false; started.resolve(); await release.promise; } };
    const input = f.deliveries.map(ack), pending = f.port.acknowledgeBatch(input);
    try {
      await Promise.race([started.promise, pending.then(() => assert.fail("no held SQL"))]);
      for (const value of input) { value.deliveryId = "changed"; value.sha256 = "f".repeat(64); }
      input.reverse(); input.length = 0; release.resolve();
      assert.deepEqual(await pending, [true, true, true]);
    } finally { release.resolve(); await Promise.allSettled([pending]); }
  });
  test(`atomic ACK ${mode}: failed commit rolls back all members`, async t => {
    const f = await setup(t, fanout), before = saved(f);
    f.owner.beforeCommit = () => { throw new Error("source commit failed"); };
    await assert.rejects(f.port.acknowledgeBatch(f.deliveries.map(ack)), /source commit failed/);
    f.owner.beforeCommit = null; assert.deepEqual(saved(f), before);
    if (fanout) assert.deepEqual(await positions(f), [0n, 0n]);
  });
  test(`atomic ACK ${mode}: malformed arrays never enter SQL or invoke accessors`, async t => {
    const f = await setup(t, fanout), before = f.owner.begins; let calls = 0;
    const entry = ack(f.deliveries[0]);
    const accessor = [entry]; Object.defineProperty(accessor, "0", { get() { calls++; return entry; } });
    const field = {}; Object.defineProperty(field, "deliveryId", { get() { calls++; return entry.deliveryId; } });
    for (const input of [[], new Array(1), new Array(257), accessor, [field], [entry, entry], {}, [Object.create(entry)]])
      await assert.rejects(f.port.acknowledgeBatch(input));
    assert.equal(calls, 0); assert.equal(f.owner.begins, before);
  });
  test(`atomic ACK ${mode}: owned savepoint can fail without discarding caller writes`, async t => {
    const f = await setup(t, fanout), before = saved(f);
    await f.owner.transaction(async tx => {
      await tx.execute("INSERT INTO items VALUES(99,'caller')");
      const box = fanout ? (await ChangesetFanout.open(tx, ["east", "west"])).forReplica("east") : new ChangesetOutbox(tx);
      const input = f.deliveries.map(ack); input[2].sha256 = "0".repeat(64);
      await assert.rejects(box.acknowledgeBatch(input));
      assert.deepEqual(saved(f), before);
    });
    assert.deepEqual(rows(f.owner.db, "SELECT value FROM items WHERE id=99"), [["caller"]]);
  });
  test(`atomic ACK ${mode}: released ACK savepoint still rolls back with enclosing owner`, async t => {
    const f = await setup(t, fanout), before = saved(f);
    await assert.rejects(f.owner.transaction(async tx => {
      const box = fanout ? (await ChangesetFanout.open(tx, ["east", "west"])).forReplica("east") : new ChangesetOutbox(tx);
      assert.deepEqual(await box.acknowledgeBatch(f.deliveries.map(ack)), [true, true, true]);
      throw new Error("outer rollback");
    }), /outer rollback/);
    assert.deepEqual(saved(f), before); if (fanout) assert.deepEqual(await positions(f), [0n, 0n]);
  });
}

test("fanout batch ACK cannot skip a source predecessor or bypass another replica", async t => {
  const f = await setup(t, true);
  await assert.rejects(f.port.acknowledgeBatch([ack(f.deliveries[0]), ack(f.deliveries[2])]), /without skipping/);
  assert.deepEqual(await positions(f), [0n, 0n]);
  await assert.rejects(f.outbox.acknowledgeBatch(f.deliveries.map(ack)), /receiver-bound/);
  await f.port.acknowledgeBatch(f.deliveries.map(ack));
  await f.group.forReplica("west").acknowledgeBatch(f.deliveries.slice(0, 2).map(ack));
  assert.deepEqual(saved(f).map(r => r[1]), [1n, 1n, 0n]);
  assert.equal((await f.group.progress()).acknowledgedThrough, 2n);
});

test("atomic ACK supports 256 retained empty identities with one commit", async t => {
  const f = await setup(t, false, 0), input = [];
  for (let n = 0; n < 256; n++) input.push((await f.outbox.record(() => {}, { tables: ["items"], deliveryId: `empty:${n}` })).delivery);
  const before = f.owner.begins;
  assert.deepEqual(await f.outbox.acknowledgeBatch(input.map(ack)), Array(256).fill(true));
  assert.equal(f.owner.begins - before, 1); assert.equal(saved(f).length, 256);
});

test("atomic bootstrap ACK retains predecessor rules and entire-window rollback", async t => {
  const f = await setup(t, false, 0); f.owner.db.exec("INSERT INTO items VALUES(1,'a'),(2,'b'),(3,'c')");
  const seed = await f.outbox.bootstrapChunks({ deliveryId: "seed", tables: ["items"], chunkRows: 1 });
  assert.equal(seed.chunks, 3);
  const input = (await f.outbox.pending()).map(ack), before = saved(f);
  await assert.rejects(f.outbox.acknowledgeBatch([input[0], input[2]]), /without skipping/);
  assert.deepEqual(saved(f), before);
  assert.deepEqual(await f.outbox.acknowledgeBatch(input), [true, true, true]);
  assert.equal((await f.outbox.bootstrapChunks({ deliveryId: "seed", tables: ["items"], chunkRows: 1 })).complete, true);
});

for (const fanout of [false, true]) test(`atomic ACK WAL reopen after lost commit response (${fanout})`, async t => {
  const path = join(mkdtempSync(join(tmpdir(), "fsqlite-atomic-ack-")), "source.db");
  let owner = new Owner(path); owner.db.exec("PRAGMA journal_mode=WAL");
  let outbox = new ChangesetOutbox(owner), group = fanout ? await ChangesetFanout.open(owner, ["east", "west"]) : null;
  const input = [];
  for (let n = 1; n <= 3; n++) input.push(ack((await outbox.record(tx => tx.execute("INSERT INTO items VALUES(?,?)", [BigInt(n), `row-${n}`]), { tables: ["items"], deliveryId: `source:${n}` })).delivery));
  owner.afterCommit = () => { throw new Error("lost source response"); };
  try { await assert.rejects((group?.forReplica("east") ?? outbox).acknowledgeBatch(input), /lost source response/); }
  finally { owner.close(); }
  owner = new Owner(path); t.after(() => owner.close()); outbox = new ChangesetOutbox(owner);
  group = fanout ? await ChangesetFanout.open(owner, ["east", "west"]) : null;
  assert.deepEqual(await (group?.forReplica("east") ?? outbox).acknowledgeBatch(input), [false, false, false]);
  if (group) {
    assert.equal((await group.progress()).acknowledgedThrough, 0n);
    assert.deepEqual(await group.forReplica("west").acknowledgeBatch(input), [true, true, true]);
  }
  assert.deepEqual(rows(owner.db, `SELECT acknowledged,length(payload) FROM ${T}`), [[1n,0n],[1n,0n],[1n,0n]]);
});
