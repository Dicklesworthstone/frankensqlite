// Actual SDK capture/outbox -> bootstrap/apply/order -> source acknowledgement.
// SQLite supplies transaction ownership; this is not Rust/WASM or power-loss certification.
import assert from "node:assert/strict";
import { test } from "node:test";
import { DatabaseSync } from "node:sqlite";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import {
  acknowledgeBootstrapInstall, ChangesetBootstrapReceiver, createBootstrapManifest,
} from "../src/changeset-bootstrap.ts";
import { ChangesetOutbox, CHANGESET_OUTBOX_TABLE } from "../src/changeset-outbox.ts";
import {
  ChangesetFanout, CHANGESET_FANOUT_TABLE, CHANGESET_FANOUT_PROGRESS_TABLE,
} from "../src/changeset-fanout.ts";
import { ChangesetOrder } from "../src/changeset-order.ts";
import { applyChangeset } from "../src/changeset-apply.ts";

const TABLE = `"${CHANGESET_OUTBOX_TABLE}"`;
const ROUTE = Object.freeze({ receiverId: "replica/🌍", orderedSourceId: "source/incarnation-1" });
const SCHEMA = "CREATE TABLE notes(id INTEGER PRIMARY KEY, body TEXT, data BLOB)";
const bootCode = kind => error => error?.code === `ERR_FSQLITE_BOOTSTRAP_${kind}`;
const outboxCode = kind => error => error?.code === `ERR_FSQLITE_OUTBOX_${kind}`;
const fanoutCode = kind => error => error?.code === `ERR_FSQLITE_FANOUT_${kind}`;
const copy = value => structuredClone(value);
const chunkId = (id, i) => i === 0 ? id : `${id}/chunk/${i}`;
const isAckWrite = sql => sql.includes("SET acknowledged=1,payload=X''") && sql.includes("seq<=?");

class Target {
  constructor(path = ":memory:", initialize = true) {
    this.db = new DatabaseSync(path);
    this.db.exec("PRAGMA foreign_keys=ON; PRAGMA recursive_triggers=ON; PRAGMA busy_timeout=0");
    if (initialize) this.db.exec(SCHEMA);
    this.active = false;
    this.transactions = 0;
    this.sql = [];
    this.before = async () => {};
    this.after = async () => {};
    this.beforeCommit = async () => {};
    this.afterCommit = async () => {};
  }
  async transaction(work) {
    assert.equal(this.active, false, "no nested independent source transaction");
    this.transactions++;
    this.active = true;
    this.db.exec("BEGIN");
    let committed = false;
    const submit = async (sql, params, read) => {
      this.sql.push(sql);
      await this.before(sql, params);
      const stmt = this.db.prepare(sql);
      const result = read ? { rowArrays: stmt.all(...params).map(Object.values) }
        : Number(stmt.run(...params).changes);
      await this.after(sql, params, result);
      return result;
    };
    try {
      const value = await work({
        execute: (sql, params = []) => submit(sql, params, false),
        query: (sql, params = []) => submit(sql, params, true),
      });
      await this.beforeCommit();
      this.db.exec("COMMIT"); committed = true;
      await this.afterCommit();
      return value;
    } catch (error) {
      if (!committed) this.db.exec("ROLLBACK");
      throw error;
    } finally { this.active = false; }
  }
  rows() {
    return this.db.prepare("SELECT CAST(id AS TEXT),hex(body),hex(data) FROM notes ORDER BY id")
      .all().map(Object.values);
  }
  entries() {
    return this.db.prepare(`SELECT CAST(seq AS TEXT),delivery_id,sha256,byte_length,change_count,scope,acknowledged,hex(payload) FROM ${TABLE} ORDER BY seq`)
      .all().map(Object.values);
  }
  close() { this.db.close(); }
}
async function setup(t, { rows = 3, source = new Target(), ordered = true, fanout = false } = {}) {
  const destination = new Target();
  t.after(() => { source.close(); destination.close(); });
  const group = fanout ? await ChangesetFanout.open(source, [ROUTE.receiverId, "slow-replica"]) : null;
  for (let i = 0; i < rows; i++)
    source.db.prepare("INSERT INTO notes VALUES(?,?,?)").run(BigInt(i + 1), `seed-${i}\0🌍`, new Uint8Array([0, i % 256, 255]));
  const outbox = new ChangesetOutbox(source);
  const config = { deliveryId: "source/seed", tables: ["notes"], chunkRows: 1 };
  const seed = await outbox.bootstrapChunks(config);
  const readChunk = async i => {
    const record = await outbox.read(chunkId(config.deliveryId, i));
    assert.ok(record?.changeset instanceof Uint8Array);
    return record.changeset;
  };
  const manifest = await createBootstrapManifest({
    receiverId: ROUTE.receiverId, deliveryId: config.deliveryId, tables: config.tables,
    chunks: seed.chunks, changes: seed.changes, byteLength: seed.byteLength,
  }, readChunk);
  const options = ordered ? { ...ROUTE } : { receiverId: ROUTE.receiverId };
  let confirmations = 0;
  const receiver = new ChangesetBootstrapReceiver(destination, {
    ...options, tables: config.tables,
    confirmCommit: async () => { assert.equal(destination.active, false); confirmations++; },
  });
  for (let i = 0; i < seed.chunks; i++) await receiver.stage(manifest, i, await readChunk(i));
  const receipt = await receiver.install(manifest);
  const ack = (r = receipt, m = manifest, controls = options, owner = source) =>
    acknowledgeBootstrapInstall(owner, m, r, controls);
  return { source, destination, outbox, config, seed, manifest, receipt, ack, options, group,
    confirmations: () => confirmations };
}

for (const ordered of [false, true]) {
  test(`complete source seed is reclaimed atomically; identities and sequence remain (${ordered ? "ordered" : "unordered"})`, async t => {
    const f = await setup(t, { ordered });
    const before = f.source.entries();
    f.source.sql = [];
    assert.equal(await f.ack(), 3);
    assert.equal(f.source.sql.filter(isAckWrite).length, 1);
    assert.deepEqual(f.source.entries(), before.map(row => [...row.slice(0, 6), 1, ""]));
    assert.deepEqual(f.destination.rows(), f.source.rows());
    assert.deepEqual(await f.outbox.pending(), []);
    assert.equal(f.source.db.prepare("SELECT seq FROM sqlite_sequence WHERE name=?").get(CHANGESET_OUTBOX_TABLE).seq, 3);
    assert.equal(await f.ack(), 0);
    assert.equal(f.confirmations(), 1, "source ACK does not contact or reapply the receiver");
  });
}

test("captured increment N+1 remains byte-identical and applies after bulk seed ACK", async t => {
  const f = await setup(t);
  const captured = await f.outbox.record(async tx => {
    await tx.execute("UPDATE notes SET body=? WHERE id=1", ["updated\0🌍"]);
    await tx.execute("DELETE FROM notes WHERE id=2");
    await tx.execute("INSERT INTO notes VALUES(?,?,?)", [9223372036854775807n, "int64", new Uint8Array([128, 255])]);
  }, { deliveryId: "source/next", tables: ["notes"] });
  assert.equal(captured.delivery.sequence, 4n);
  const saved = await f.outbox.read("source/next");
  assert.equal(await f.ack(), 3);
  assert.deepEqual(await f.outbox.read("source/next"), saved);
  assert.deepEqual((await f.outbox.pending()).map(d => d.sequence), [4n]);
  const order = new ChangesetOrder(f.destination, { receiverId: ROUTE.receiverId, sourceId: ROUTE.orderedSourceId });
  const applied = await order.apply({ ...saved.delivery, changeset: saved.changeset },
    (inside, bytes) => applyChangeset(inside, bytes, { tables: ["notes"], deliveryId: "source/next" }));
  assert.equal(applied.replayed, false);
  assert.equal(applied.applied, 3);
  assert.equal((await order.head()).sequence, 4n);
  assert.deepEqual(f.destination.rows(), f.source.rows());
  await f.outbox.acknowledge("source/next", saved.delivery.sha256);
  assert.equal(await f.ack(), 0);
  const replay = await f.outbox.bootstrapChunks(f.config);
  assert.equal(replay.replayed, true);
  assert.equal(replay.complete, true);
  assert.equal(replay.acknowledgedChunks, 3);
});

for (const prefix of [1, 2, 3]) {
  test(`partial ordinary ACK prefix ${prefix} completes without requiring reclaimed bodies`, async t => {
    const f = await setup(t);
    for (let i = 0; i < prefix; i++) {
      const id = chunkId(f.manifest.deliveryId, i), item = await f.outbox.read(id);
      await f.outbox.acknowledge(id, item.delivery.sha256);
    }
    assert.equal(await f.ack(), 3 - prefix);
    assert.equal(await f.ack(), 0);
    assert.ok(f.source.entries().every(row => row[6] === 1 && row[7] === ""));
  });
}

for (const [field, value] of [
  ["protocol", "fsqlite-changeset-v1"], ["receiverId", "another"], ["deliveryId", "other/root"],
  ["sha256", "a".repeat(64)], ["chunks", 2], ["changes", 0], ["byteLength", 1],
  ["installed", false], ["confirmed", false], ["replayed", "yes"],
]) {
  test(`reject forged install ${field} before entering source SQL`, async t => {
    const f = await setup(t), before = f.source.entries(), n = f.source.transactions;
    await assert.rejects(f.ack({ ...f.receipt, [field]: value }), bootCode("STATE"));
    assert.equal(f.source.transactions, n);
    assert.deepEqual(f.source.entries(), before);
  });
}
for (const order of [undefined, { protocol: "legacy", streamId: ROUTE.orderedSourceId, sequence: "3" },
  { protocol: "fsqlite-ordered-changeset-v1", streamId: "other", sequence: "3" },
  { protocol: "fsqlite-ordered-changeset-v1", streamId: ROUTE.orderedSourceId, sequence: "03" },
  { protocol: "fsqlite-ordered-changeset-v1", streamId: ROUTE.orderedSourceId, sequence: 3 }]) {
  test(`reject missing or forged ordered prefix ${JSON.stringify(order)}`, async t => {
    const f = await setup(t), before = f.source.entries();
    await assert.rejects(f.ack({ ...f.receipt, order }));
    assert.deepEqual(f.source.entries(), before);
  });
}

test("caller route cannot be inferred from incoming receipt; ordered ACK cannot downgrade", async t => {
  const f = await setup(t);
  await assert.rejects(f.ack(f.receipt, f.manifest, { receiverId: "other" }), bootCode("INPUT"));
  await assert.rejects(f.ack(f.receipt, f.manifest, { receiverId: ROUTE.receiverId }), bootCode("INPUT"));
  assert.ok(f.source.entries().every(row => row[6] === 0));
});

test("staging progress, inherited ACK fields and accessors do not authorize reclamation", async t => {
  const f = await setup(t);
  await assert.rejects(f.ack({ receivedChunks: 3, installed: false }));
  await assert.rejects(f.ack(Object.create(f.receipt)), bootCode("INPUT"));
  let called = 0;
  const value = { ...f.receipt };
  Object.defineProperty(value, "sha256", { get() { called++; throw Error("must not run"); } });
  await assert.rejects(f.ack(value), bootCode("INPUT"));
  assert.equal(called, 0);
  assert.ok(f.source.entries().every(row => row[6] === 0));
});

test("manifest, receipt and route are captured before asynchronous source admission", async t => {
  const f = await setup(t), m = copy(f.manifest), r = copy(f.receipt), opts = { ...f.options };
  let release;
  const gate = new Promise(resolve => { release = resolve; });
  const owner = { transaction: async (work, controls) => { await gate; return f.source.transaction(work, controls); } };
  const pending = f.ack(r, m, opts, owner);
  m.tables[0] = "other"; m.sha256 = "b".repeat(64); r.confirmed = false;
  r.order.sequence = "99"; opts.receiverId = "other"; opts.orderedSourceId = "other";
  release();
  assert.equal(await pending, 3);
});

for (const sql of [
  `DELETE FROM ${TABLE} WHERE seq=2`,
  `UPDATE ${TABLE} SET delivery_id='other' WHERE seq=2`,
  `UPDATE ${TABLE} SET payload=zeroblob(byte_length) WHERE seq=3`,
  `UPDATE ${TABLE} SET sha256='${"a".repeat(64)}' WHERE seq=3`,
  `UPDATE ${TABLE} SET byte_length=byte_length+1 WHERE seq=3`,
  `UPDATE ${TABLE} SET change_count=99 WHERE seq=3`,
  `UPDATE ${TABLE} SET acknowledged=1,payload=X'' WHERE seq=2`,
  `UPDATE ${TABLE} SET scope=json_set(scope,'$.stream.summary.chunks',2) WHERE seq=1`,
  `UPDATE ${TABLE} SET scope=json_set(scope,'$.tables[0]','other') WHERE seq=2`,
]) {
  test(`source corruption rejects before any additional ACK: ${sql}`, async t => {
    const f = await setup(t);
    f.source.db.exec(sql);
    const before = f.source.entries();
    await assert.rejects(f.ack());
    assert.deepEqual(f.source.entries(), before);
  });
}

test("coherent source digest mutation still fails the original full manifest hash", async t => {
  const f = await setup(t);
  for (const row of await f.outbox.pending()) await f.outbox.acknowledge(row.deliveryId, row.sha256);
  f.source.db.exec(`UPDATE ${TABLE} SET sha256='${"f".repeat(64)}' WHERE seq=2`);
  await assert.rejects(f.ack(), bootCode("CORRUPT"));
});

test("matching totals cannot substitute another selected table scope", async t => {
  const f = await setup(t), manifest = { ...f.manifest, tables: ["other"] };
  await assert.rejects(f.ack(f.receipt, manifest), bootCode("STATE"));
  assert.ok(f.source.entries().every(row => row[6] === 0));
});

test("unknown and explicitly forgotten seed identities never create empty authority", async t => {
  const f = await setup(t), empty = new Target(); t.after(() => empty.close());
  await assert.rejects(f.ack(f.receipt, f.manifest, f.options, empty), bootCode("STATE"));
  assert.equal(empty.db.prepare("SELECT count(*) AS n FROM sqlite_schema WHERE name=?").get(CHANGESET_OUTBOX_TABLE).n, 0);
  await f.ack();
  await f.outbox.forgetBootstrapChunks(f.config.deliveryId, f.seed.sha256);
  await assert.rejects(f.ack(), bootCode("STATE"));
  assert.deepEqual(f.source.entries(), []);
});

for (const namespace of ["main", "temp"]) {
  test(`${namespace} source-outbox triggers refuse bulk ACK before execution`, async t => {
    const f = await setup(t);
    f.source.db.exec(`CREATE ${namespace === "temp" ? "TEMP " : ""}TRIGGER bad AFTER UPDATE ON ${TABLE} BEGIN SELECT 1; END`);
    const before = f.source.entries();
    await assert.rejects(f.ack(), outboxCode("SCHEMA"));
    assert.deepEqual(f.source.entries(), before);
  });
}

test("one installed replica cannot reclaim bytes required by a fanout member", async t => {
  const f = await setup(t, { fanout: true }), before = f.source.entries();
  await assert.rejects(f.ack(), fanoutCode("STATE"));
  assert.deepEqual(f.source.entries(), before);
  assert.equal((await f.group.progress()).acknowledgedThrough, 0n);
  const first = await f.outbox.read(f.config.deliveryId);
  await f.group.forReplica(ROUTE.receiverId).acknowledge(first.delivery.deliveryId, first.delivery.sha256);
  assert.notEqual((await f.outbox.read(first.delivery.deliveryId)).changeset, null);
});
for (const name of [CHANGESET_FANOUT_TABLE, CHANGESET_FANOUT_PROGRESS_TABLE]) {
  test(`missing fanout companion ${name} is not treated as a single-recipient source`, async t => {
    const f = await setup(t, { fanout: true });
    f.source.db.exec(`DROP TABLE "${name}"`);
    const before = f.source.entries();
    await assert.rejects(f.ack(), fanoutCode("CORRUPT"));
    assert.deepEqual(f.source.entries(), before);
  });
}

for (const where of ["read", "write", "commit"]) {
  test(`failure at ${where} leaves every source seed byte and ACK intact`, async t => {
    const f = await setup(t), before = f.source.entries();
    if (where === "read") f.source.after = async sql => { if (sql.startsWith("SELECT payload")) throw Error("injected read"); };
    if (where === "write") f.source.after = async sql => { if (isAckWrite(sql)) throw Error("injected write"); };
    if (where === "commit") f.source.beforeCommit = async () => { throw Error("injected commit"); };
    await assert.rejects(f.ack(), /injected/);
    f.source.after = async () => {}; f.source.beforeCommit = async () => {};
    assert.deepEqual(f.source.entries(), before);
    assert.equal(await f.ack(), 3);
  });
}

test("real deferred-COMMIT failure rolls back bulk reclamation", async t => {
  const f = await setup(t), before = f.source.entries();
  f.source.db.exec("CREATE TABLE parent(id PRIMARY KEY); CREATE TABLE child(id REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)");
  f.source.beforeCommit = async () => { f.source.db.exec("INSERT INTO child VALUES(99)"); };
  await assert.rejects(f.ack(), /FOREIGN KEY/);
  f.source.beforeCommit = async () => {};
  assert.deepEqual(f.source.entries(), before);
  assert.equal(f.source.db.prepare("SELECT count(*) AS n FROM child").get().n, 0);
  assert.equal(await f.ack(), 3);
});

for (const at of ["before", "read", "write"]) {
  test(`cancellation at ${at} does not leave partial source acknowledgement`, async t => {
    const f = await setup(t), before = f.source.entries(), controller = new AbortController();
    if (at === "before") controller.abort();
    f.source.after = async sql => {
      if ((at === "read" && sql.startsWith("SELECT payload")) || (at === "write" && isAckWrite(sql))) controller.abort();
    };
    await assert.rejects(f.ack(f.receipt, f.manifest, { ...f.options, signal: controller.signal }), bootCode("CANCELLED"));
    f.source.after = async () => {};
    assert.deepEqual(f.source.entries(), before);
    assert.equal(await f.ack(), 3);
  });
}

test("deadline during started SQL waits for settlement before rollback", async t => {
  const f = await setup(t), before = f.source.entries();
  let settled = false;
  f.source.after = async sql => {
    if (isAckWrite(sql)) { await new Promise(resolve => setTimeout(resolve, 1100)); settled = true; }
  };
  await assert.rejects(f.ack(f.receipt, f.manifest, { ...f.options, timeoutMs: 1000 }), bootCode("TIMEOUT"));
  f.source.after = async () => {};
  assert.equal(settled, true);
  assert.deepEqual(f.source.entries(), before);
});

test("cancellation arriving after COMMIT does not claim committed ACKs rolled back", async t => {
  const f = await setup(t), controller = new AbortController();
  f.source.afterCommit = async () => { controller.abort(); };
  assert.equal(await f.ack(f.receipt, f.manifest, { ...f.options, signal: controller.signal }), 3);
  f.source.afterCommit = async () => {};
  assert.equal(await f.ack(), 0);
});

test("lost source COMMIT response recovers the exact acknowledged seed after reopen", async t => {
  const path = join(mkdtempSync(join(tmpdir(), "fsqlite-ack-reopen-")), "source.db");
  const f = await setup(t, { source: new Target(path) });
  f.source.afterCommit = async () => { throw Error("lost source response"); };
  await assert.rejects(f.ack(), /lost source response/);
  const reopened = new Target(path, false); t.after(() => reopened.close());
  assert.equal(await f.ack(f.receipt, f.manifest, f.options, reopened), 0);
  assert.equal(reopened.entries().length, 3);
  assert.ok(reopened.entries().every(row => row[6] === 1 && row[7] === ""));
});

test("source acknowledgement stays provisional inside an enclosing transaction", async t => {
  const f = await setup(t), before = f.source.entries();
  await assert.rejects(f.source.transaction(async tx => {
    const inside = { transaction: async work => work(tx) };
    assert.equal(await f.ack(f.receipt, f.manifest, f.options, inside), 3);
    throw Error("outer transaction rolls back");
  }), /outer transaction rolls back/);
  assert.deepEqual(f.source.entries(), before);
  assert.equal(await f.ack(), 3);
});

test("an incorrect affected-row count rolls back the real bulk mutation", async t => {
  const f = await setup(t), before = f.source.entries();
  const owner = { transaction: work => f.source.transaction(tx => work({
    query: tx.query,
    execute: async (sql, params) => {
      const changed = await tx.execute(sql, params);
      return isAckWrite(sql) ? changed - 1 : changed;
    },
  })) };
  await assert.rejects(f.ack(f.receipt, f.manifest, f.options, owner), bootCode("CORRUPT"));
  assert.deepEqual(f.source.entries(), before);
});

for (const rows of [0, 1, 65]) {
  test(`empty/single/multi-page seed (${rows} rows) verifies bounded chunks then one ACK mutation`, async t => {
    const f = await setup(t, { rows });
    f.source.sql = [];
    let largest = 0;
    f.source.after = async (_sql, _params, result) => {
      if (result?.rowArrays) largest = Math.max(largest, result.rowArrays.length);
    };
    assert.equal(await f.ack(), Math.max(1, rows));
    assert.ok(largest <= 32, "source helper never fetches an unbounded metadata or payload array");
    assert.equal(f.source.sql.filter(isAckWrite).length, 1);
    f.source.sql = [];
    assert.equal(await f.ack(), 0);
    assert.equal(f.source.sql.filter(sql => sql.startsWith("SELECT payload")).length, 0);
  });
}

for (const journal of ["DELETE", "WAL"]) {
  for (const phase of ["write", "before-commit", "after-commit"]) {
    test(`SIGKILL at ${phase} (${journal}) keeps the source seed wholly pending or wholly acknowledged`, async t => {
      const path = join(mkdtempSync(join(tmpdir(), "fsqlite-ack-crash-")), "source.db");
      const source = new Target(path); source.db.exec(`PRAGMA journal_mode=${journal}`);
      const f = await setup(t, { source });
      const script = `
        import assert from 'node:assert/strict';
        import { DatabaseSync } from 'node:sqlite';
        import { acknowledgeBootstrapInstall } from './packages/sdk/src/changeset-bootstrap.ts';
        ${Target.toString()}
        const target = new Target(process.env.SOURCE_PATH, false);
        const crash = () => process.kill(process.pid, 'SIGKILL');
        target.after = async sql => { if (process.env.PHASE === 'write' && sql.includes("SET acknowledged=1,payload=X''")) crash(); };
        target.beforeCommit = async () => { if (process.env.PHASE === 'before-commit') crash(); };
        target.afterCommit = async () => { if (process.env.PHASE === 'after-commit') crash(); };
        await acknowledgeBootstrapInstall(target, JSON.parse(process.env.MANIFEST), JSON.parse(process.env.RECEIPT), JSON.parse(process.env.OPTIONS));
        throw Error('crash boundary not reached');
      `;
      const child = spawnSync(process.execPath, ["--experimental-transform-types",
        "--experimental-loader=./packages/sdk/tests/helpers/fanout-source-loader.mjs", "--input-type=module", "--eval", script], {
        cwd: fileURLToPath(new URL("../../../", import.meta.url)), timeout: 10_000, encoding: "utf8",
        env: { ...process.env, SOURCE_PATH: path, PHASE: phase, MANIFEST: JSON.stringify(f.manifest),
          RECEIPT: JSON.stringify(f.receipt), OPTIONS: JSON.stringify(f.options) },
      });
      assert.equal(child.error, undefined); assert.equal(child.signal, "SIGKILL", child.stderr);
      const reopened = new Target(path, false); t.after(() => reopened.close());
      const committed = phase === "after-commit";
      assert.ok(reopened.entries().every(row => row[6] === Number(committed) && (committed ? row[7] === "" : row[7].length > 0)));
      assert.equal(await f.ack(f.receipt, f.manifest, f.options, reopened), committed ? 0 : 3);
      assert.equal(reopened.db.prepare("PRAGMA integrity_check").get().integrity_check, "ok");
    });
  }
}
