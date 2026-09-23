// Production bootstrap -> SQL changeset apply -> order ledger on reference SQLite.
// Run with Node 22.16+ --experimental-transform-types and fanout-source-loader.mjs.
// These are SDK integration tests, not FrankenSQLite Rust/WASM certification.
import assert from "node:assert/strict";
import { test } from "node:test";
import { DatabaseSync } from "node:sqlite";
import { createHash } from "node:crypto";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import {
  ChangesetBootstrapReceiver, createBootstrapManifest,
  CHANGESET_BOOTSTRAP_STATE_TABLE, CHANGESET_BOOTSTRAP_CHUNKS_TABLE,
} from "../src/changeset-bootstrap.ts";
import {
  ChangesetOrder, CHANGESET_ORDER_TABLE, CHANGESET_ORDER_HEAD_TABLE,
} from "../src/changeset-order.ts";
import { applyChangeset, CHANGESET_RECEIPTS_TABLE } from "../src/changeset-apply.ts";
import { decodeChangeset } from "../src/changeset-codec.ts";

const STATE = `"${CHANGESET_BOOTSTRAP_STATE_TABLE}"`;
const CHUNKS = `"${CHANGESET_BOOTSTRAP_CHUNKS_TABLE}"`;
const ORDER = `"${CHANGESET_ORDER_TABLE}"`;
const HEAD = `"${CHANGESET_ORDER_HEAD_TABLE}"`;
const schema = "CREATE TABLE notes(id INTEGER PRIMARY KEY, body TEXT, data BLOB)";
const binding = { receiverId: "replica/α", sourceId: "source/incarnation/1" };
const hash = bytes => createHash("sha256").update(bytes).digest("hex");
const codes = (...expected) => error => expected.includes(error?.code);
const orderOptions = { receiverId: binding.receiverId, sourceId: binding.sourceId };
const errorCode = suffix => codes(`ERR_FSQLITE_ORDER_${suffix}`);
const bootCode = suffix => codes(`ERR_FSQLITE_BOOTSTRAP_${suffix}`);
const until = async predicate => {
  for (let i = 0; i < 200; i++) {
    if (predicate()) return;
    await new Promise(resolve => setTimeout(resolve, 1));
  }
  assert.fail("Test barrier did not open");
};

class SqliteTarget {
  constructor(path = ":memory:", initialize = true) {
    this.db = new DatabaseSync(path);
    this.db.exec("PRAGMA foreign_keys=ON; PRAGMA recursive_triggers=ON; PRAGMA busy_timeout=0");
    if (initialize) this.db.exec(schema);
    this.active = false;
    this.before = async () => {};
    this.after = async () => {};
    this.beforeCommit = async () => {};
    this.afterCommit = async () => {};
    this.sql = [];
  }
  executor() {
    return {
      execute: async (sql, params = []) => {
        this.sql.push(sql);
        await this.before(sql, params);
        const result = Number(this.db.prepare(sql).run(...params).changes);
        await this.after(sql, params, result);
        return result;
      },
      query: async (sql, params = []) => {
        this.sql.push(sql);
        await this.before(sql, params);
        const rows = this.db.prepare(sql).all(...params).map(row => Object.values(row));
        await this.after(sql, params, rows);
        return { rowArrays: rows };
      },
    };
  }
  async transaction(work) {
    assert.equal(this.active, false, "production code must not open a second independent transaction");
    this.active = true;
    this.db.exec("BEGIN");
    let committed = false;
    try {
      const value = await work(this.executor());
      await this.beforeCommit();
      this.db.exec("COMMIT");
      committed = true;
      await this.afterCommit();
      return value;
    } catch (error) {
      if (!committed) this.db.exec("ROLLBACK");
      throw error;
    } finally { this.active = false; }
  }
  rows() {
    return this.db.prepare("SELECT CAST(id AS TEXT), hex(body), hex(data) FROM notes ORDER BY id")
      .all().map(row => Object.values(row));
  }
  exists(table) {
    return !!this.db.prepare("SELECT 1 FROM sqlite_schema WHERE name=?").get(table);
  }
  close() { this.db.close(); }
}
function receiver(target, extra = {}) {
  return new ChangesetBootstrapReceiver(target, {
    receiverId: binding.receiverId, tables: ["notes"], orderedSourceId: binding.sourceId,
    confirmCommit: async () => { assert.equal(target.active, false); }, ...extra,
  });
}
function apply(order, bytes, sequence, deliveryId = `source/change/${sequence}`) {
  return order.apply({ sequence, deliveryId, sha256: hash(bytes), changeset: bytes },
    (inside, owned) => applyChangeset(inside, owned, { tables: ["notes"], deliveryId }));
}
function capture(db, work) {
  const session = db.createSession();
  try { work(); return session.changeset(); }
  finally { session.close(); }
}
async function fixture(t, { chunks = 3, target, empty = false, root = "source/baseline", extra = {} } = {}) {
  const source = new DatabaseSync(":memory:");
  source.exec(schema);
  target ??= new SqliteTarget();
  t.after(() => { source.close(); target.close(); });
  const payloads = [];
  for (let i = 0; i < chunks; i++) {
    payloads.push(capture(source, () => {
      if (!empty) source.prepare("INSERT INTO notes VALUES(?,?,?)")
        .run(BigInt(i + 1), `seed-${i}🌍`, new Uint8Array([0, i % 256, 255]));
    }));
  }
  const manifest = await createBootstrapManifest({
    receiverId: binding.receiverId, deliveryId: root, tables: ["notes"], chunks,
    byteLength: payloads.reduce((n, b) => n + b.byteLength, 0), changes: empty ? 0 : chunks,
  }, async index => payloads[index]);
  const bootstrap = receiver(target, extra);
  const stage = async () => {
    for (let i = 0; i < chunks; i++) await bootstrap.stage(manifest, i, payloads[i]);
  };
  return { source, target, payloads, manifest, bootstrap, stage, order: new ChangesetOrder(target, orderOptions) };
}
function assertStaged(f) {
  assert.deepEqual(f.target.rows(), []);
  assert.equal(f.target.db.prepare(`SELECT installed FROM ${STATE}`).get().installed, 0);
  assert.equal(f.target.db.prepare(`SELECT sum(length(payload)) AS n FROM ${CHUNKS}`).get().n,
    f.manifest.byteLength);
}
function assertInstalled(f) {
  assert.equal(f.target.db.prepare(`SELECT installed FROM ${STATE}`).get().installed, 1);
  assert.equal(f.target.db.prepare(`SELECT sum(length(payload)) AS n FROM ${CHUNKS}`).get().n, 0);
  assert.equal(f.target.db.prepare("PRAGMA integrity_check").get().integrity_check, "ok");
}

test("atomic bootstrap publishes the original source prefix and admits increment N+1", async t => {
  const f = await fixture(t);
  await f.stage();
  assertStaged(f);
  assert.equal(f.target.exists(CHANGESET_ORDER_TABLE), false, "staging is not order publication");
  const receipt = await f.bootstrap.install(f.manifest);
  assert.deepEqual(receipt.order, {
    protocol: "fsqlite-ordered-changeset-v1", streamId: binding.sourceId, sequence: "3",
  });
  assert.equal((await f.order.head()).sequence, 3n);
  assertInstalled(f);
  const delta = capture(f.source, () => f.source.exec("UPDATE notes SET body='incremental' WHERE id=1"));
  assert.equal((await apply(f.order, delta, 4n)).applied, 1);
  assert.deepEqual(f.target.rows(), f.source.prepare(
    "SELECT CAST(id AS TEXT), hex(body), hex(data) FROM notes ORDER BY id").all().map(Object.values));
  assert.equal(f.target.db.prepare(`SELECT count(*) AS n FROM "${CHANGESET_RECEIPTS_TABLE}"`).get().n, 1);
});

test("all seed retries retain exact decisions, without running INSERTs again", async t => {
  const f = await fixture(t);
  await f.stage(); await f.bootstrap.install(f.manifest);
  for (let i = 0; i < f.payloads.length; i++) {
    const id = i === 0 ? f.manifest.deliveryId : `${f.manifest.deliveryId}/chunk/${i}`;
    const result = await f.order.apply({ sequence: BigInt(i + 1), deliveryId: id,
      sha256: hash(f.payloads[i]), changeset: f.payloads[i] }, () => { assert.fail("Seed was reapplied"); });
    assert.equal(result.replayed, true);
    assert.equal(result.applied, 1);
    assert.equal(result.omitted, 0);
  }
  await assert.rejects(apply(f.order, f.payloads[0], 5n), errorCode("GAP"));
  await assert.rejects(apply(f.order, f.payloads[0], 1n, "wrong-root"), errorCode("REUSE"));
});

test("installed replay after incremental delivery preserves the newer head and data", async t => {
  let confirmations = 0;
  const f = await fixture(t, { extra: { confirmCommit: async () => { confirmations++; } } });
  await f.stage(); await f.bootstrap.install(f.manifest);
  const delta = capture(f.source, () => f.source.exec("DELETE FROM notes WHERE id=2"));
  await apply(f.order, delta, 4n);
  const before = f.target.rows();
  const receipt = await f.bootstrap.install(f.manifest);
  assert.equal(receipt.replayed, true);
  assert.equal(receipt.order.sequence, "3", "receipt names seed frontier, not newer incremental tip");
  assert.equal((await f.order.head()).sequence, 4n);
  assert.equal(confirmations, 2);
  assert.deepEqual(f.target.rows(), before);
});

for (const chunks of [1, 4, 33]) {
  test(`empty native baseline still occupies its ${chunks} original outbox slots`, async t => {
    const f = await fixture(t, { chunks, empty: true });
    await f.stage(); await f.bootstrap.install(f.manifest);
    assert.equal((await f.order.head()).sequence, BigInt(chunks));
    assert.deepEqual(f.target.rows(), []);
    const delta = capture(f.source, () => f.source.exec("INSERT INTO notes VALUES(1,'first',X'')"));
    await apply(f.order, delta, BigInt(chunks + 1));
    assert.equal(f.target.rows().length, 1);
  });
}

test("ordered policy is captured once and persisted before installation", async t => {
  const f = await fixture(t);
  const options = { receiverId: binding.receiverId, tables: ["notes"],
    orderedSourceId: binding.sourceId, confirmCommit: async () => {} };
  const bootstrap = new ChangesetBootstrapReceiver(f.target, options);
  options.orderedSourceId = "changed-after-construction";
  await bootstrap.stage(f.manifest, 0, f.payloads[0]);
  for (const orderedSourceId of [undefined, "different", binding.sourceId.toUpperCase()]) {
    const wrong = receiver(f.target, { orderedSourceId });
    await assert.rejects(wrong.status(f.manifest), bootCode("STATE"));
    await assert.rejects(wrong.stage(f.manifest, 0, f.payloads[0]), bootCode("STATE"));
    await assert.rejects(wrong.install(f.manifest), bootCode("STATE"));
  }
  for (let i = 1; i < f.payloads.length; i++) await bootstrap.stage(f.manifest, i, f.payloads[i]);
  await bootstrap.install(f.manifest);
  assert.equal((await f.order.head()).sourceId, binding.sourceId);
});

test("legacy bootstrap remains unordered; adding order on replay is refused", async t => {
  const f = await fixture(t, { extra: { orderedSourceId: undefined } });
  await f.stage();
  const receipt = await f.bootstrap.install(f.manifest);
  assert.equal(Object.hasOwn(receipt, "order"), false);
  assert.equal(f.target.exists(CHANGESET_ORDER_TABLE), false);
  assert.equal((await f.bootstrap.install(f.manifest)).replayed, true);
  await assert.rejects(receiver(f.target).install(f.manifest), bootCode("STATE"));
});

test("preinitialized matching genesis is usable but a nonzero ledger cannot be rebased", async t => {
  const f = await fixture(t);
  await f.order.initialize();
  const seed = capture(f.source, () => {});
  await apply(f.order, seed, 1n);
  await f.stage();
  await assert.rejects(f.bootstrap.install(f.manifest), errorCode("REUSE"));
  assertStaged(f);
  assert.equal((await f.order.head()).sequence, 1n);
});

test("a live incremental endpoint cannot publish staged seed chunks separately", async t => {
  const f = await fixture(t);
  await f.order.initialize();
  await f.bootstrap.stage(f.manifest, 0, f.payloads[0]);
  await assert.rejects(apply(f.order, f.payloads[0], 1n, f.manifest.deliveryId), errorCode("GAP"));
  assert.deepEqual(f.target.rows(), []);
  for (let i = 1; i < f.payloads.length; i++) await f.bootstrap.stage(f.manifest, i, f.payloads[i]);
  await assert.rejects(apply(f.order, f.payloads[0], 1n, f.manifest.deliveryId), errorCode("GAP"));
  assert.deepEqual(f.target.rows(), []);
  await f.bootstrap.install(f.manifest);
  assert.equal((await apply(f.order, f.payloads[0], 1n, f.manifest.deliveryId)).replayed, true);
});

test("reinitializing lost order tables cannot bypass an installed bootstrap", async t => {
  const f = await fixture(t);
  await f.stage(); await f.bootstrap.install(f.manifest);
  f.target.db.exec(`DROP TABLE ${ORDER}; DROP TABLE ${HEAD}`);
  await f.order.initialize(); // Explicit genesis enrollment is not proof of seed history.
  const empty = capture(f.source, () => {});
  await assert.rejects(apply(f.order, empty, 1n), errorCode("CORRUPT"));
  assert.equal((await f.order.head()).sequence, 0n);
  assert.equal(f.target.rows().length, 3);
});

for (const change of [
  `DELETE FROM ${STATE}`,
  `DROP TABLE ${STATE}`,
  `DROP TABLE ${CHUNKS}`,
  `UPDATE ${STATE} SET installed=2`,
  `UPDATE ${STATE} SET received=2`,
  `UPDATE ${CHUNKS} SET byte_length=999 WHERE idx=2`,
  `UPDATE ${CHUNKS} SET change_count=0 WHERE idx=2`,
  `UPDATE ${CHUNKS} SET payload=X'00' WHERE idx=2`,
]) {
  test(`incremental application refuses a damaged bootstrap binding: ${change}`, async t => {
    const f = await fixture(t);
    await f.stage(); await f.bootstrap.install(f.manifest);
    f.target.db.exec(change);
    const delta = capture(f.source, () => f.source.exec("UPDATE notes SET body='must not apply' WHERE id=1"));
    const before = f.target.rows();
    await assert.rejects(apply(f.order, delta, 4n), errorCode("CORRUPT"));
    assert.deepEqual(f.target.rows(), before);
  });
}

test("preinitialized matching genesis advances with a successful install", async t => {
  const f = await fixture(t);
  await f.order.initialize(); await f.stage(); await f.bootstrap.install(f.manifest);
  assert.equal((await f.order.head()).sequence, 3n);
});

for (const mismatch of [{ sourceId: "other-source" }, { receiverId: "other-receiver" }]) {
  test(`foreign ledger binding rolls back every baseline INSERT: ${JSON.stringify(mismatch)}`, async t => {
    const f = await fixture(t);
    await new ChangesetOrder(f.target, { ...binding, ...mismatch }).initialize();
    await f.stage();
    await assert.rejects(f.bootstrap.install(f.manifest), errorCode("BINDING"));
    assertStaged(f);
  });
}

for (const fault of ["order-insert", "head-update", "payload-clear", "installed-mark", "commit"]) {
  test(`fault at ${fault} rolls back rows, order, publication and reclamation`, async t => {
    const f = await fixture(t);
    await f.stage();
    const inject = (sql, params) => {
      if ((fault === "order-insert" && sql.startsWith(`INSERT OR ABORT INTO main.${ORDER}`) && params[0] === 2n) ||
          (fault === "head-update" && sql.startsWith(`UPDATE OR ABORT main.${HEAD}`)) ||
          (fault === "payload-clear" && sql.includes("SET payload=X''")) ||
          (fault === "installed-mark" && sql.includes("SET installed=1"))) throw Error("injected failure");
    };
    f.target.after = inject;
    if (fault === "commit") f.target.beforeCommit = () => { throw Error("injected failure"); };
    await assert.rejects(f.bootstrap.install(f.manifest), /injected failure/);
    f.target.after = async () => {}; f.target.beforeCommit = async () => {};
    assertStaged(f);
    assert.equal(f.target.exists(CHANGESET_ORDER_TABLE), false);
    assert.equal(f.target.exists(CHANGESET_ORDER_HEAD_TABLE), false);
    await f.bootstrap.install(f.manifest);
    assert.equal((await f.order.head()).sequence, 3n);
  });
}

for (const corrupt of [
  `DROP TABLE ${ORDER}`, `DROP TABLE ${HEAD}`, `DROP TABLE ${ORDER}; DROP TABLE ${HEAD}`,
  `DELETE FROM ${ORDER} WHERE seq=1`, `DELETE FROM ${ORDER} WHERE seq=3`,
  `UPDATE ${ORDER} SET sha256='${"a".repeat(64)}' WHERE seq=1`,
  `UPDATE ${ORDER} SET applied=0 WHERE seq=1`,
  `UPDATE ${CHUNKS} SET sha256='${"a".repeat(64)}' WHERE idx=0`,
  `UPDATE ${CHUNKS} SET payload=X'00' WHERE idx=0`,
  `DELETE FROM ${CHUNKS} WHERE idx=0`,
]) {
  test(`installed replay refuses damaged history without reconstructing it: ${corrupt}`, async t => {
    let confirmations = 0;
    const f = await fixture(t, { extra: { confirmCommit: async () => { confirmations++; } } });
    await f.stage(); await f.bootstrap.install(f.manifest);
    f.target.db.exec(corrupt);
    const before = f.target.rows();
    await assert.rejects(f.bootstrap.install(f.manifest));
    assert.equal(confirmations, 1);
    assert.deepEqual(f.target.rows(), before);
  });
}

test("missing history cannot be laundered by coherently changing both prefix and head", async t => {
  const f = await fixture(t);
  await f.stage(); await f.bootstrap.install(f.manifest);
  // Counts/extent/head are still consistent, but manifest binds the original metadata.
  f.target.db.exec(`UPDATE ${ORDER} SET sha256='${"a".repeat(64)}' WHERE seq=1;
    UPDATE ${CHUNKS} SET sha256='${"a".repeat(64)}' WHERE idx=0`);
  await assert.rejects(f.bootstrap.install(f.manifest), bootCode("CORRUPT"));
});

test("cancel during prefix publication drains SQL and rolls back all install effects", async t => {
  const f = await fixture(t);
  await f.stage();
  const controller = new AbortController();
  f.target.after = async (sql, params) => {
    if (sql.startsWith(`INSERT OR ABORT INTO main.${ORDER}`) && params[0] === 2n) controller.abort();
  };
  await assert.rejects(f.bootstrap.install(f.manifest, { signal: controller.signal }), bootCode("CANCELLED"));
  f.target.after = async () => {};
  assertStaged(f);
  assert.equal(f.target.exists(CHANGESET_ORDER_TABLE), false);
  await f.bootstrap.install(f.manifest);
});

test("cancel after SQL COMMIT still drains confirmation; replay does not repeat application", async t => {
  const controller = new AbortController();
  let confirmations = 0;
  const f = await fixture(t, { extra: { confirmCommit: async () => { confirmations++; } } });
  await f.stage();
  f.target.afterCommit = async () => { controller.abort(); };
  await assert.rejects(f.bootstrap.install(f.manifest, { signal: controller.signal }), bootCode("CANCELLED"));
  f.target.afterCommit = async () => {};
  assertInstalled(f);
  assert.equal((await f.order.head()).sequence, 3n);
  assert.equal(confirmations, 1);
  assert.equal((await f.bootstrap.install(f.manifest)).replayed, true);
  assert.equal(confirmations, 2);
});

test("lost COMMIT response and file reopen retain a single baseline-to-stream handoff", async t => {
  const path = join(mkdtempSync(join(tmpdir(), "fsqlite-bootstrap-order-")), "receiver.db");
  const target = new SqliteTarget(path);
  const f = await fixture(t, { target });
  await f.stage();
  target.afterCommit = () => { throw Error("lost commit response"); };
  await assert.rejects(f.bootstrap.install(f.manifest), /lost commit response/);
  target.afterCommit = async () => {};
  const reopened = new SqliteTarget(path, false); t.after(() => reopened.close());
  const boot = receiver(reopened), order = new ChangesetOrder(reopened, orderOptions);
  assert.equal((await boot.install(f.manifest)).replayed, true);
  const delta = capture(f.source, () => f.source.exec("UPDATE notes SET body='after reopen' WHERE id=1"));
  await apply(order, delta, 4n);
  assert.equal((await order.head()).sequence, 4n);
  assert.equal(reopened.rows().length, 3);
});

test("other connections see either no baseline/order or the complete published pair", async t => {
  const path = join(mkdtempSync(join(tmpdir(), "fsqlite-bootstrap-isolation-")), "receiver.db");
  const target = new SqliteTarget(path);
  const f = await fixture(t, { target });
  await f.stage();
  const observer = new SqliteTarget(path, false); t.after(() => observer.close());
  let seen = false, release;
  const gate = new Promise(resolve => { release = resolve; });
  target.after = async sql => {
    if (sql.includes("SET installed=1")) { seen = true; await gate; }
  };
  const pending = f.bootstrap.install(f.manifest);
  await until(() => seen);
  assert.deepEqual(observer.rows(), []);
  assert.equal(observer.exists(CHANGESET_ORDER_TABLE), false);
  release(); await pending;
  assert.equal(observer.rows().length, 3);
  assert.equal((await new ChangesetOrder(observer, orderOptions).head()).sequence, 3n);
});

test("scope remains busy through failed confirmation; recovery reconfirms without reapplying", async t => {
  let started = false, release;
  const gate = new Promise(resolve => { release = resolve; });
  const f = await fixture(t, { extra: { confirmCommit: async () => { started = true; await gate; throw Error("storage unavailable"); } } });
  await f.stage();
  const pending = f.bootstrap.install(f.manifest);
  await until(() => started);
  await assert.rejects(f.bootstrap.install(f.manifest), bootCode("BUSY"));
  release(); await assert.rejects(pending, bootCode("CONFIRM"));
  assertInstalled(f);
  const recovered = receiver(f.target);
  assert.equal((await recovered.install(f.manifest)).replayed, true);
});

test("native deferred foreign-key COMMIT failure also rolls back the order prefix", async t => {
  const target = new SqliteTarget();
  target.db.exec("CREATE TABLE parents(id INTEGER PRIMARY KEY); CREATE TABLE kids(id INTEGER PRIMARY KEY, parent INTEGER REFERENCES parents(id) DEFERRABLE INITIALLY DEFERRED)");
  const source = new DatabaseSync(":memory:");
  t.after(() => { source.close(); target.close(); });
  source.exec("PRAGMA foreign_keys=OFF; CREATE TABLE kids(id INTEGER PRIMARY KEY, parent INTEGER)");
  const chunk = capture(source, () => source.exec("INSERT INTO kids VALUES(1,99)"));
  const manifest = await createBootstrapManifest({ receiverId: binding.receiverId,
    deliveryId: "source/fk-seed", tables: ["kids"], chunks: 1, changes: 1, byteLength: chunk.byteLength }, async () => chunk);
  const bootstrap = receiver(target, { tables: ["kids"] });
  await bootstrap.stage(manifest, 0, chunk);
  await assert.rejects(bootstrap.install(manifest), /FOREIGN KEY/);
  assert.equal(target.db.prepare("SELECT count(*) AS n FROM kids").get().n, 0);
  assert.equal(target.exists(CHANGESET_ORDER_TABLE), false);
  assert.equal(target.db.prepare(`SELECT installed FROM ${STATE}`).get().installed, 0);
  target.db.exec("INSERT INTO parents VALUES(99)");
  await bootstrap.install(manifest);
  assert.equal((await new ChangesetOrder(target, orderOptions).head()).sequence, 1n);
});

test("metadata publication does not load seed payloads or rescan the ledger per chunk", async t => {
  const f = await fixture(t, { chunks: 100 });
  await f.stage(); f.target.sql = [];
  await f.bootstrap.install(f.manifest);
  const populationScans = f.target.sql.filter(sql => sql.includes("CAST(min(seq) AS TEXT)")).length;
  assert.equal(populationScans, 2, "one initial and one final order-head validation");
  assert.equal(f.target.sql.filter(sql => sql.startsWith("SELECT payload FROM")).length, 100);
  f.target.sql = [];
  await f.bootstrap.install(f.manifest);
  assert.equal(f.target.sql.filter(sql => sql.startsWith("SELECT payload FROM")).length, 0);
});

test("native scalar payloads survive baseline followed by production incremental application", async t => {
  const f = await fixture(t, { chunks: 1, empty: true });
  const bytes = capture(f.source, () => f.source.prepare("INSERT INTO notes VALUES(?,?,?)")
    .run(9223372036854775807n, "nul\0unicode🌍", new Uint8Array([0, 255, 128])));
  const manifest = await createBootstrapManifest({ ...f.manifest, changes: 1, byteLength: bytes.byteLength }, async () => bytes);
  await f.bootstrap.stage(manifest, 0, bytes); await f.bootstrap.install(manifest);
  assert.equal(decodeChangeset(bytes)[0].changes[0].new[0], 9223372036854775807n);
  const delta = capture(f.source, () => f.source.prepare("UPDATE notes SET body=? WHERE id=?")
    .run("after\0change", 9223372036854775807n));
  await apply(f.order, delta, 2n);
  const oracle = f.source.prepare("SELECT CAST(id AS TEXT), hex(body), hex(data) FROM notes ORDER BY id").all().map(Object.values);
  assert.deepEqual(f.target.rows(), oracle);
});

// Kill a separate Node process at real SQL boundaries, not a simulated rollback.
// On reopen, C SQLite recovers its own journal/WAL. No power-loss claim is made.
for (const journal of ["DELETE", "WAL"]) {
  for (const phase of ["rows", "prefix", "head", "reclaim", "before-commit", "after-commit", "confirm"]) {
    test(`process death at ${phase} (${journal}) recovers the baseline and order together`, async t => {
      const path = join(mkdtempSync(join(tmpdir(), "fsqlite-seed-crash-")), "receiver.db");
      const target = new SqliteTarget(path);
      target.db.exec(`PRAGMA journal_mode=${journal}`);
      const f = await fixture(t, { target });
      await f.stage();
      const script = `
        import assert from 'node:assert/strict';
        import { DatabaseSync } from 'node:sqlite';
        import { ChangesetBootstrapReceiver } from './packages/sdk/src/changeset-bootstrap.ts';
        const schema = ${JSON.stringify(schema)};
        ${SqliteTarget.toString()}
        const target = new SqliteTarget(process.env.CRASH_DB, false);
        const phase = process.env.CRASH_PHASE;
        const crash = () => process.kill(process.pid, 'SIGKILL');
        target.after = async (sql, params) => {
          if ((phase === 'rows' && sql.startsWith('INSERT OR ABORT INTO main."notes"')) ||
              (phase === 'prefix' && sql.startsWith('INSERT OR ABORT INTO main.${ORDER}') && params[0] === 2n) ||
              (phase === 'head' && sql.startsWith('UPDATE OR ABORT main.${HEAD}')) ||
              (phase === 'reclaim' && sql.includes("SET payload=X''"))) crash();
        };
        target.beforeCommit = async () => { if (phase === 'before-commit') crash(); };
        target.afterCommit = async () => { if (phase === 'after-commit') crash(); };
        const receiver = new ChangesetBootstrapReceiver(target, {
          receiverId: ${JSON.stringify(binding.receiverId)}, tables: ['notes'],
          orderedSourceId: ${JSON.stringify(binding.sourceId)},
          confirmCommit: async () => { if (phase === 'confirm') crash(); },
        });
        await receiver.install(JSON.parse(process.env.CRASH_MANIFEST));
        throw Error('Crash boundary was never reached');
      `;
      const child = spawnSync(process.execPath, [
        "--experimental-transform-types",
        "--experimental-loader=./packages/sdk/tests/helpers/fanout-source-loader.mjs",
        "--input-type=module", "--eval", script,
      ], {
        cwd: fileURLToPath(new URL("../../../", import.meta.url)),
        env: { ...process.env, CRASH_DB: path, CRASH_PHASE: phase, CRASH_MANIFEST: JSON.stringify(f.manifest) },
        timeout: 10_000, encoding: "utf8",
      });
      assert.equal(child.error, undefined);
      assert.equal(child.signal, "SIGKILL", child.stderr);
      const reopened = new SqliteTarget(path, false); t.after(() => reopened.close());
      const committed = phase === "after-commit" || phase === "confirm";
      assert.equal(reopened.rows().length, committed ? 3 : 0);
      assert.equal(reopened.exists(CHANGESET_ORDER_TABLE), committed);
      assert.equal(reopened.db.prepare(`SELECT installed FROM ${STATE}`).get().installed, Number(committed));
      assert.equal(reopened.db.prepare(`SELECT sum(length(payload)) AS n FROM ${CHUNKS}`).get().n,
        committed ? 0 : f.manifest.byteLength);
      const receipt = await receiver(reopened).install(f.manifest);
      assert.equal(receipt.replayed, committed);
      assert.equal((await new ChangesetOrder(reopened, orderOptions).head()).sequence, 3n);
      assert.equal(reopened.rows().length, 3);
      assert.equal(reopened.db.prepare("PRAGMA integrity_check").get().integrity_check, "ok");
    });
  }
}

for (const attack of ["drop", "downgrade", "change-scope"]) {
  test(`incremental callbacks cannot ${attack} their bootstrap authority`, async t => {
    const f = await fixture(t);
    await f.stage(); await f.bootstrap.install(f.manifest);
    const delta = capture(f.source, () => f.source.exec("UPDATE notes SET body='must rollback' WHERE id=1"));
    const before = f.target.rows();
    await assert.rejects(f.order.apply({ sequence: 4n, deliveryId: "source/change/4",
      sha256: hash(delta), changeset: delta },
      (inside, bytes) => applyChangeset(inside, bytes, {
        tables: ["notes"], deliveryId: "source/change/4",
        onRebase: async tx => {
          if (attack === "drop") {
            await tx.execute(`DROP TABLE ${STATE}`);
            await tx.execute(`DROP TABLE ${CHUNKS}`);
          } else {
            const value = JSON.parse(f.target.db.prepare(`SELECT manifest FROM ${STATE}`).get().manifest);
            if (attack === "downgrade") delete value.orderedSourceId;
            else value.tables = ["unrelated"];
            await tx.execute(`UPDATE ${STATE} SET manifest=?`, [JSON.stringify(value)]);
          }
        },
      })), errorCode("CORRUPT"));
    assert.deepEqual(f.target.rows(), before);
    assert.equal((await f.order.head()).sequence, 3n);
    assert.equal(f.target.exists(CHANGESET_RECEIPTS_TABLE), false);
    assert.equal((await f.bootstrap.install(f.manifest)).replayed, true);
  });
}
