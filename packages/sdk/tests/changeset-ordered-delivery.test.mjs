// Actual production ledger/adapters on native SQLite. Not Rust/WASM certification.
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import test from "node:test";
import { ChangesetOrder, CHANGESET_ORDER_TABLE } from "../src/changeset-order.ts";
import { CHANGESET_ORDER_PROTOCOL, createOrderedChangesetReceiver } from "../src/changeset-ordered-delivery.ts";

const ID = { receiverId: "east", sourceId: "source:incarnation-1" };
const HASH = bytes => createHash("sha256").update(bytes).digest("hex");
const code = suffix => error => error?.code === `ERR_FSQLITE_ORDERED_${suffix}`;
const gateCode = suffix => error => error?.code === `ERR_FSQLITE_ORDER_${suffix}`;
const pause = () => { let resolve; const promise = new Promise(r => { resolve = r; }); return { promise, resolve }; };
function message(sequence = 1n, body = `body-${sequence}`) {
  const db = new DatabaseSync(":memory:");
  db.exec("CREATE TABLE notes(id INTEGER PRIMARY KEY, body TEXT)");
  const session = db.createSession();
  db.prepare("INSERT INTO notes VALUES (?,?)").run(sequence, body);
  const changeset = session.changeset(); session.close(); db.close();
  return { protocol: "fsqlite-changeset-v1", receiverId: ID.receiverId,
    deliveryId: `source:op-${sequence}`, sha256: HASH(changeset), changeset,
    order: { protocol: CHANGESET_ORDER_PROTOCOL, streamId: ID.sourceId, sequence: sequence.toString() } };
}
function open(path = ":memory:") {
  const db = new DatabaseSync(path);
  db.exec("PRAGMA foreign_keys=ON; CREATE TABLE IF NOT EXISTS notes(id INTEGER PRIMARY KEY, body TEXT)");
  const state = { active: false, failCommit: false, applications: 0, confirmations: 0, after: null };
  const executor = {
    execute: async (sql, params = []) => Number(db.prepare(sql).run(...params).changes),
    query: async (sql, params = []) => {
      const s = db.prepare(sql); s.setReadBigInts(true);
      return { rowArrays: s.all(...params).map(row => Object.values(row)) };
    },
  };
  const target = { async transaction(work) {
    assert.equal(state.active, false); state.active = true; db.exec("BEGIN");
    try {
      const result = await work(executor);
      if (state.failCommit) { state.failCommit = false; throw new Error("injected commit failure"); }
      db.exec("COMMIT"); return result;
    } catch (error) { db.exec("ROLLBACK"); throw error; }
    finally { state.active = false; }
  } };
  // Reference apply adapter. Uses native session bytes and real inbox/transaction
  // storage, but is NOT the SDK's production applyChangeset implementation.
  const apply = (scoped, msg, options) => scoped.transaction(async tx => {
    state.applications++;
    await tx.execute("CREATE TABLE IF NOT EXISTS inbox(id TEXT PRIMARY KEY, digest TEXT)");
    assert.equal(db.applyChangeset(msg.changeset), true);
    await tx.execute("INSERT INTO inbox VALUES (?,?)", [msg.deliveryId, msg.sha256]);
    if (state.after) await state.after(tx, msg);
    return { applied: 1, omitted: 0, replayed: false };
  }, options);
  const confirmCommit = async () => {
    assert.equal(state.active, false, "confirmation must follow outer COMMIT");
    state.confirmations++;
  };
  const rows = () => db.prepare("SELECT id,body FROM notes ORDER BY id").all().map(row => Object.values(row));
  return { db, state, target, apply, confirmCommit, rows, close: () => db.close() };
}
async function setup(t, options = {}) {
  const f = open(); t.after(f.close);
  f.order = new ChangesetOrder(f.target, ID); await f.order.initialize();
  f.receiver = await createOrderedChangesetReceiver(f.order, { apply: f.apply, confirmCommit: f.confirmCommit, ...options });
  return f;
}

test("real session bytes commit with ledger and inbox before confirmed ordered ACK", async t => {
  const f = await setup(t);
  for (let n = 1n; n <= 12n; n++) {
    const msg = message(n), ack = await f.receiver.receive(msg);
    assert.deepEqual(ack, { protocol: msg.protocol, receiverId: msg.receiverId,
      deliveryId: msg.deliveryId, sha256: msg.sha256, byteLength: msg.changeset.length,
      applied: 1, omitted: 0, replayed: false, confirmed: true, order: msg.order });
    assert.ok(Object.isFrozen(ack)); assert.ok(Object.isFrozen(ack.order));
  }
  assert.equal(f.rows().length, 12); assert.equal(f.state.confirmations, 12);
});
test("historical replay returns original decisions and reconfirms without application", async t => {
  const f = await setup(t);
  await f.receiver.receive(message()); await f.receiver.receive(message(2n));
  const replay = await f.receiver.receive(message());
  assert.equal(replay.replayed, true); assert.equal(replay.order.sequence, "1");
  assert.equal(f.state.applications, 2); assert.equal(f.state.confirmations, 3);
  assert.equal((await f.order.head()).sequence, 2n);
});
test("gap rejects before application; missing predecessor unblocks delivery", async t => {
  const f = await setup(t);
  await assert.rejects(f.receiver.receive(message(2n)), gateCode("GAP"));
  assert.equal(f.state.applications, 0); assert.equal(f.state.confirmations, 0);
  await f.receiver.receive(message()); await f.receiver.receive(message(2n));
  assert.equal(f.rows().length, 2);
});
test("missing ledger is never enrolled by receiver construction", async t => {
  const f = open(); t.after(f.close);
  await assert.rejects(createOrderedChangesetReceiver(new ChangesetOrder(f.target, ID), f), gateCode("UNINITIALIZED"));
});
test("confirmation failure after COMMIT recovers by exact replay after file reopen", async t => {
  const path = join(mkdtempSync(join(tmpdir(), "fsqlite-ordered-delivery-")), "receiver.db");
  let f = open(path), order = new ChangesetOrder(f.target, ID); await order.initialize();
  const receiver = await createOrderedChangesetReceiver(order, { apply: f.apply, confirmCommit: async () => { throw new Error("lost confirmation"); } });
  await assert.rejects(receiver.receive(message()), /lost confirmation/);
  assert.equal(f.rows().length, 1); f.close();
  f = open(path); t.after(f.close); order = new ChangesetOrder(f.target, ID);
  const recovered = await createOrderedChangesetReceiver(order, f);
  assert.equal((await recovered.receive(message())).replayed, true);
  assert.equal(f.state.applications, 0); assert.equal(f.state.confirmations, 1);
});
test("failed COMMIT rolls back application, inbox and ordering", async t => {
  const f = await setup(t); f.state.failCommit = true;
  await assert.rejects(f.receiver.receive(message()), /injected commit failure/);
  assert.deepEqual(f.rows(), []); assert.equal((await f.order.head()).sequence, 0n);
  assert.equal(f.state.confirmations, 0); await f.receiver.receive(message());
});
test("deferred foreign-key COMMIT failure does not advance ledger or confirm", async t => {
  const f = await setup(t);
  f.db.exec("CREATE TABLE p(id INTEGER PRIMARY KEY); CREATE TABLE c(id INTEGER REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)");
  f.state.after = tx => tx.execute("INSERT INTO c VALUES(1)");
  await assert.rejects(f.receiver.receive(message()), /FOREIGN KEY/);
  assert.deepEqual(f.rows(), []); assert.equal((await f.order.head()).sequence, 0n);
  assert.equal(f.state.confirmations, 0);
});
test("busy admission remains closed through suspended confirmation", async t => {
  const p = pause(), started = pause();
  const f = await setup(t, { confirmCommit: async () => { started.resolve(); await p.promise; } });
  const pending = f.receiver.receive(message()); await started.promise;
  await assert.rejects(f.receiver.receive(message(2n)), code("BUSY"));
  assert.equal(f.state.active, false); p.resolve(); await pending;
});
test("pre-cancelled work performs no SQL or confirmation", async t => {
  const f = await setup(t), c = new AbortController(); c.abort();
  await assert.rejects(f.receiver.receive(message(), { signal: c.signal }), code("CANCELLED"));
  assert.equal(f.state.applications, 0); assert.equal(f.state.confirmations, 0);
});
test("cancellation inside application rolls back the same transaction", async t => {
  const f = await setup(t), c = new AbortController(); f.state.after = async () => c.abort();
  await assert.rejects(f.receiver.receive(message(), { signal: c.signal }));
  assert.deepEqual(f.rows(), []); assert.equal((await f.order.head()).sequence, 0n);
});
test("cancellation after COMMIT waits for confirmation but withholds ACK", async t => {
  const p = pause(), started = pause(), c = new AbortController();
  const f = await setup(t, { confirmCommit: async () => { started.resolve(); await p.promise; } });
  const pending = f.receiver.receive(message(), { signal: c.signal });
  await started.promise; c.abort(); let settled = false;
  void pending.then(() => { settled = true; }, () => { settled = true; });
  await new Promise(r => setImmediate(r)); assert.equal(settled, false);
  p.resolve(); await assert.rejects(pending, code("CANCELLED"));
  assert.equal(f.rows().length, 1); assert.equal((await f.order.head()).sequence, 1n);
});
for (const sequence of ["0", "01", "-1", "+1", "1.0", "1e0", "9223372036854775808", 1, 1n, null])
  test(`reject noncanonical sequence ${String(sequence)} (${typeof sequence})`, async t => {
    const f = await setup(t), msg = message(); msg.order.sequence = sequence;
    await assert.rejects(f.receiver.receive(msg), code("INPUT")); assert.equal(f.state.applications, 0);
  });
for (const [name, alter] of [
  ["missing order", m => { delete m.order; }], ["wrong stream", m => { m.order.streamId = "other"; }],
  ["wrong protocol", m => { m.order.protocol = "v0"; }], ["wrong receiver", m => { m.receiverId = "west"; }],
  ["wrong digest", m => { m.sha256 = "f".repeat(64); }], ["missing digest", m => { delete m.sha256; }],
]) test(`receiver refuses ${name}`, async t => {
  const f = await setup(t), msg = message(); alter(msg);
  await assert.rejects(f.receiver.receive(msg)); assert.equal(f.state.applications, 0);
});
test("own-property admission never invokes incoming accessors", async t => {
  const f = await setup(t), msg = message(); let calls = 0;
  Object.defineProperty(msg, "order", { get() { calls++; throw new Error("accessor"); } });
  await assert.rejects(f.receiver.receive(msg), code("INPUT")); assert.equal(calls, 0);
});
test("capture bytes and sequence before first await", async t => {
  const f = await setup(t), msg = message(); const pending = f.receiver.receive(msg);
  msg.changeset.fill(0); msg.order.sequence = "99";
  assert.equal((await pending).order.sequence, "1"); assert.deepEqual(f.rows(), [[1, "body-1"]]);
});
test("intrinsic typed-array size prevents a subclass bypassing byte admission", async t => {
  const f = await setup(t, { maxMessageBytes: 1 }), msg = message();
  class HiddenSize extends Uint8Array { get byteLength() { return 0; } }
  msg.changeset = new HiddenSize(msg.changeset);
  await assert.rejects(f.receiver.receive(msg), code("INPUT")); assert.equal(f.state.applications, 0);
});
test("ignored target cannot advance ordering", async t => {
  const f = await setup(t, { apply: async () => ({ applied: 1, omitted: 0, replayed: false }) });
  await assert.rejects(f.receiver.receive(message()), code("RECEIPT"));
  assert.equal((await f.order.head()).sequence, 0n); assert.equal(f.state.confirmations, 0);
});
test("premature application response drains pending SQL and rolls back", async t => {
  const p = pause(), started = pause(); let f;
  f = await setup(t, { apply: async target => {
    void target.transaction(async tx => {
      await tx.execute("INSERT INTO notes VALUES(1,'pending')"); started.resolve(); await p.promise;
      return { applied: 1, omitted: 0, replayed: false };
    });
    return { applied: 1, omitted: 0, replayed: false };
  } });
  const pending = f.receiver.receive(message()); await started.promise;
  let settled = false; void pending.catch(() => { settled = true; });
  await new Promise(r => setImmediate(r)); assert.equal(settled, false);
  p.resolve(); await assert.rejects(pending, code("RECEIPT"));
  assert.deepEqual(f.rows(), []); assert.equal(f.state.active, false); assert.equal((await f.order.head()).sequence, 0n);
});
test("forged application decision counts roll back actual SQL", async t => {
  const f = await setup(t);
  const receiver = await createOrderedChangesetReceiver(f.order, { ...f, apply: async (...args) => {
    const result = await f.apply(...args); return { ...result, omitted: 1 };
  } });
  await assert.rejects(receiver.receive(message()), code("RECEIPT")); assert.deepEqual(f.rows(), []);
});
test("stored ledger corruption blocks replay and confirmation", async t => {
  const f = await setup(t); await f.receiver.receive(message());
  f.db.exec(`DELETE FROM "${CHANGESET_ORDER_TABLE}" WHERE seq=1`);
  await assert.rejects(f.receiver.receive(message()), gateCode("CORRUPT")); assert.equal(f.state.confirmations, 1);
});
test("receiver captures application and confirmation configuration before opening", async t => {
  const f = await setup(t), opts = { apply: f.apply, confirmCommit: f.confirmCommit };
  const pending = createOrderedChangesetReceiver(f.order, opts);
  opts.apply = () => { throw new Error("changed"); }; opts.confirmCommit = opts.apply;
  const receiver = await pending; await receiver.receive(message()); assert.equal(f.state.confirmations, 1);
});
