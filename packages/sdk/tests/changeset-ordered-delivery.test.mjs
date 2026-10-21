// Actual production ledger/adapters on native SQLite. Not Rust/WASM certification.
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import test from "node:test";
import { ChangesetOrder, CHANGESET_ORDER_TABLE } from "../src/changeset-order.ts";
import { CHANGESET_ORDER_PROTOCOL, createOrderedChangesetReceiver, createOrderedChangesetTransport } from "../src/changeset-ordered-delivery.ts";

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

function sourceFor(msg, overrides = {}) {
  return { receiverId: ID.receiverId, reads: 0,
    async read(id) {
      this.reads++; assert.equal(id, msg.deliveryId);
      return { delivery: { sequence: BigInt(msg.order.sequence), deliveryId: msg.deliveryId,
        sha256: msg.sha256, byteLength: msg.changeset.length, changes: 1, acknowledged: false, ...overrides },
        changeset: new Uint8Array(msg.changeset) };
    },
  };
}
const transportOptions = deliver => ({ receiverId: ID.receiverId, streamId: ID.sourceId, deliver });
const fakeAck = msg => ({ protocol: msg.protocol, receiverId: msg.receiverId, deliveryId: msg.deliveryId,
  sha256: msg.sha256, byteLength: msg.changeset.length, applied: 1, omitted: 0,
  replayed: false, confirmed: true, order: { ...msg.order } });

test("source-bound transport reaches actual ledger and returns matching confirmed order", async t => {
  const f = await setup(t), msg = message(), source = sourceFor(msg);
  const transport = createOrderedChangesetTransport(source, transportOptions((m, opts) => {
    assert.equal(f.state.active, false); return f.receiver.receive(m, opts);
  }));
  // Ignore a caller-supplied sequence: the retained source metadata is authoritative.
  const ack = await transport({ ...msg, order: { ...msg.order, sequence: "999" } }, {});
  assert.equal(ack.order.sequence, "1"); assert.equal(source.reads, 1);
  assert.deepEqual(f.rows(), [[1, "body-1"]]);
  assert.equal((await transport(msg, {})).replayed, true); assert.equal(f.state.applications, 1);
});
test("lost transport ACK reuses exact source sequence after receiver commit", async t => {
  const f = await setup(t), msg = message(); let lost = true;
  const transport = createOrderedChangesetTransport(sourceFor(msg), transportOptions(async (m, opts) => {
    const ack = await f.receiver.receive(m, opts);
    if (lost) { lost = false; throw new Error("lost wire response"); } return ack;
  }));
  await assert.rejects(transport(msg, {}), /lost wire response/);
  assert.equal((await transport(msg, {})).replayed, true);
  assert.equal(f.state.applications, 1); assert.equal(f.state.confirmations, 2);
});
for (const sequence of [9007199254740993n, (1n << 63n) - 1n])
  test(`source int64 ${sequence} survives JSON receipt roundtrip without precision loss`, async () => {
    const msg = message();
    const transport = createOrderedChangesetTransport(sourceFor(msg, { sequence }), transportOptions(async m => {
      assert.equal(m.order.sequence, sequence.toString()); return JSON.parse(JSON.stringify(fakeAck(m)));
    }));
    assert.equal((await transport(msg, {})).order.sequence, sequence.toString());
  });
for (const [name, alter] of [
  ["missing order", a => { delete a.order; }], ["legacy order", a => { a.order.protocol = "v0"; }],
  ["wrong stream", a => { a.order.streamId = "other"; }], ["wrong sequence", a => { a.order.sequence = "2"; }],
  ["wrong receiver", a => { a.receiverId = "west"; }], ["wrong delivery", a => { a.deliveryId = "other"; }],
  ["wrong digest", a => { a.sha256 = "0".repeat(64); }], ["wrong length", a => { a.byteLength++; }],
  ["unconfirmed", a => { a.confirmed = false; }], ["wrong total", a => { a.applied = 2; }],
  ["negative count", a => { a.omitted = -1; }], ["nonboolean replay", a => { a.replayed = 1; }],
]) test(`source never accepts ${name} ACK`, async () => {
  const msg = message();
  const transport = createOrderedChangesetTransport(sourceFor(msg), transportOptions(async m => {
    const ack = fakeAck(m); alter(ack); return ack;
  }));
  await assert.rejects(transport(msg, {}));
});
for (const override of [{ sequence: 0n }, { sequence: 1 }, { sequence: 1n << 63n },
  { deliveryId: "other" }, { sha256: "0".repeat(64) }, { byteLength: 0 }, { acknowledged: 0 }, { changes: -1 }])
  test(`invalid retained metadata ${Object.keys(override)[0]}=${String(Object.values(override)[0])} refuses transport`, async () => {
    const msg = message(); let sent = 0;
    const transport = createOrderedChangesetTransport(sourceFor(msg, override), transportOptions(async m => { sent++; return fakeAck(m); }));
    await assert.rejects(transport(msg, {})); assert.equal(sent, 0);
  });
test("missing source identity refuses delivery", async () => {
  let sent = 0; const msg = message();
  const transport = createOrderedChangesetTransport({ read: async () => null }, transportOptions(async m => { sent++; return fakeAck(m); }));
  await assert.rejects(transport(msg, {}), code("RECEIPT")); assert.equal(sent, 0);
});
test("receiver-bound source cannot be configured for another replica", () => {
  assert.throws(() => createOrderedChangesetTransport(sourceFor(message()), {
    ...transportOptions(async m => fakeAck(m)), receiverId: "west",
  }), code("INPUT"));
});
test("source configuration and buffers are captured before asynchronous transport", async () => {
  const msg = message(), source = sourceFor(message()), opts = transportOptions(async m => fakeAck(m));
  const transport = createOrderedChangesetTransport(source, opts);
  opts.deliver = async () => { throw new Error("replacement"); }; source.read = opts.deliver;
  const pending = transport(msg, {}); msg.changeset.fill(0);
  const ack = await pending; assert.equal(ack.sha256, message().sha256);
});
test("transport cancellation waits for active delivery to settle", async () => {
  const p = pause(), started = pause(), c = new AbortController(), msg = message();
  const transport = createOrderedChangesetTransport(sourceFor(msg), transportOptions(async m => {
    started.resolve(); await p.promise; return fakeAck(m);
  }));
  const pending = transport(msg, { signal: c.signal }); await started.promise; c.abort();
  let settled = false; void pending.catch(() => { settled = true; });
  await assert.rejects(transport(msg, {}), code("BUSY"));
  await new Promise(r => setImmediate(r)); assert.equal(settled, false);
  p.resolve(); await assert.rejects(pending, code("CANCELLED"));
});
test("transport timeout withholds late ACK without abandoning active delivery", async () => {
  const p = pause(), started = pause(), msg = message();
  const transport = createOrderedChangesetTransport(sourceFor(msg), transportOptions(async m => {
    started.resolve(); await p.promise; return fakeAck(m);
  }));
  const pending = transport(msg, { timeoutMs: 100 }); await started.promise;
  let settled = false; void pending.catch(() => { settled = true; });
  await new Promise(r => setTimeout(r, 120)); assert.equal(settled, false);
  p.resolve(); await assert.rejects(pending, code("TIMEOUT"));
});
test("ordered omission counts remain explicit and unchanged for pump policy", async () => {
  const msg = message();
  const transport = createOrderedChangesetTransport(sourceFor(msg), transportOptions(async m => ({ ...fakeAck(m), applied: 0, omitted: 1 })));
  const ack = await transport(msg, {}); assert.equal(ack.applied, 0); assert.equal(ack.omitted, 1);
});
test("forged receipt accessors are rejected without invoking them", async () => {
  const msg = message(); let calls = 0;
  const transport = createOrderedChangesetTransport(sourceFor(msg), transportOptions(async m => {
    const ack = fakeAck(m); Object.defineProperty(ack, "order", { get() { calls++; throw new Error("getter"); } }); return ack;
  }));
  await assert.rejects(transport(msg, {})); assert.equal(calls, 0);
});
test("recovery allows a fresh call after invalid acknowledgement", async () => {
  const msg = message(); let valid = false;
  const transport = createOrderedChangesetTransport(sourceFor(msg), transportOptions(async m => {
    const ack = fakeAck(m); if (!valid) delete ack.order; return ack;
  }));
  await assert.rejects(transport(msg, {})); valid = true;
  assert.equal((await transport(msg, {})).confirmed, true);
});


// Atomic reception executes production application, not the reference adapter
// used above. SQL, savepoints, receipts and decision BLOBs are real SQLite.
import { applyChangeset, CHANGESET_RECEIPTS_TABLE } from "../src/changeset-apply.ts";
import { encodeChangeset, encodePatchset, decodeRebaseInfo } from "../src/changeset-codec.ts";
import { FrankenSQLiteError } from "../src/errors.ts";

const batchSchema = "CREATE TABLE IF NOT EXISTS notes(id INTEGER PRIMARY KEY,body TEXT UNIQUE);" +
  "CREATE TABLE IF NOT EXISTS decisions(id TEXT PRIMARY KEY,info BLOB)";
const sqlArrays = (db, sql = "SELECT * FROM notes ORDER BY id") => {
  const stmt = db.prepare(sql); stmt.setReadBigInts(true); stmt.setReturnArrays(true);
  return stmt.all();
};
const tick = () => new Promise(r => setImmediate(r));
class BatchReceiverOwner {
  constructor(path = ":memory:", ddl = batchSchema) {
    this.db = new DatabaseSync(path);
    this.db.exec("PRAGMA foreign_keys=ON;PRAGMA recursive_triggers=ON;" + ddl);
    this.depth = 0; this.serial = 0; this.events = [];
    this.beforeSql = null; this.beforeRelease = null; this.afterCommit = null;
  }
  async transaction(work) {
    const depth = this.depth++, name = `receiver_${++this.serial}`;
    this.db.exec(depth ? `SAVEPOINT ${name}` : "BEGIN");
    this.events.push(["begin", depth]);
    let active = true, committed = false;
    const check = () => assert.ok(active, "SQL owner has expired");
    const tx = {
      execute: async (sql, params = []) => {
        check(); this.events.push(["execute", depth, sql]);
        await this.beforeSql?.("execute", sql, depth);
        try { return Number(this.db.prepare(sql).run(...params).changes); }
        catch (error) {
          if (error.code === "ERR_SQLITE_ERROR" && (error.errcode & 255) === 19)
            throw new FrankenSQLiteError({ code: "SQLITE_CONSTRAINT", sqliteCode: 19,
              extendedCode: error.errcode, message: error.message });
          throw error;
        }
      },
      query: async (sql, params = []) => {
        check(); this.events.push(["query", depth, sql]);
        await this.beforeSql?.("query", sql, depth);
        const stmt = this.db.prepare(sql); stmt.setReadBigInts(true); stmt.setReturnArrays(true);
        return { rowArrays: stmt.all(...params) };
      },
      transaction: child => { check(); return this.transaction(child); },
    };
    try {
      const value = await work(tx); active = false;
      await this.beforeRelease?.(depth);
      this.db.exec(depth ? `RELEASE ${name}` : "COMMIT"); committed = true;
      this.events.push(["release", depth]);
      if (depth === 0) await this.afterCommit?.();
      return value;
    } catch (error) {
      active = false;
      if (!committed) {
        this.db.exec(depth ? `ROLLBACK TO ${name}; RELEASE ${name}` : "ROLLBACK");
        this.events.push(["rollback", depth]);
      }
      throw error;
    } finally { active = false; this.depth--; }
  }
  close() { this.db.close(); }
}
async function batchSetup(t, config = {}) {
  const owner = new BatchReceiverOwner(config.path, config.ddl); t.after(() => owner.close());
  if (config.wal) owner.db.exec("PRAGMA journal_mode=WAL");
  const order = new ChangesetOrder(owner, ID); await order.initialize();
  const f = { owner, order, calls: [], hooks: [], confirms: 0, applicationOptions: {},
    confirmHook: null, rows: sql => sqlArrays(owner.db, sql) };
  const apply = (target, msg, options) => {
    f.calls.push(msg.deliveryId);
    return applyChangeset(target, msg.changeset, {
      tables: config.tables ?? ["notes"], deliveryId: msg.deliveryId, ...options,
      ...f.applicationOptions,
    });
  };
  const confirmCommit = async () => {
    assert.equal(owner.depth, 0, "batch confirmation ran before the outer transaction settled");
    f.confirms++; await f.confirmHook?.();
  };
  f.receiver = await createOrderedChangesetReceiver(order, { apply, confirmCommit, ...config.receiver });
  f.apply = apply; f.confirmCommit = confirmCommit;
  owner.events.length = 0;
  return f;
}
function noBatchCommit(f) {
  assert.equal(f.confirms, 0); assert.equal(f.owner.depth, 0);
  assert.deepEqual(f.rows(`SELECT seq FROM "${CHANGESET_ORDER_TABLE}"`), [[0n]]);
  if (f.rows(`SELECT name FROM sqlite_schema WHERE name='${CHANGESET_RECEIPTS_TABLE}'`).length)
    assert.deepEqual(f.rows(`SELECT * FROM "${CHANGESET_RECEIPTS_TABLE}"`), []);
}
const batchEnvelope = (sequence, changeset) => ({ ...message(sequence), changeset, sha256: HASH(changeset) });
function nativeBatchMessage(sequence, ddl, sql) {
  // Construct independently valid wire fragments; receiver FK enforcement stays ON.
  const db = new DatabaseSync(":memory:");
  db.exec("PRAGMA foreign_keys=OFF;" + ddl); const session = db.createSession();
  try { db.exec(sql); return batchEnvelope(sequence, session.changeset()); }
  finally { session.close(); db.close(); }
}

test("batch receiver: production rows, nested decision BLOBs and receipts precede one confirmation", async t => {
  const f = await batchSetup(t), messages = [message(), message(2n), message(3n)];
  f.applicationOptions.onRebase = (tx, info) => {
    const id = f.calls.at(-1); f.hooks.push(id);
    return tx.transaction(child => info.length
      ? child.execute("INSERT INTO decisions VALUES(?,?)", [id, info])
      : child.execute("INSERT INTO decisions VALUES(?,zeroblob(0))", [id]));
  };
  f.confirmHook = () => {
    assert.equal(f.rows().length, 3); assert.equal(f.rows("SELECT * FROM decisions").length, 3);
    assert.equal(f.rows(`SELECT * FROM "${CHANGESET_RECEIPTS_TABLE}"`).length, 3);
  };
  const receipts = await f.receiver.receiveBatch(messages);
  assert.deepEqual(receipts, messages.map(fakeAck));
  assert.ok(Object.isFrozen(receipts));
  for (const receipt of receipts) { assert.ok(Object.isFrozen(receipt)); assert.ok(Object.isFrozen(receipt.order)); }
  for (const [, bytes] of f.rows("SELECT * FROM decisions")) assert.deepEqual(decodeRebaseInfo(bytes), []);
  assert.equal(f.confirms, 1);
  assert.equal(f.owner.events.filter(([kind, depth]) => kind === "begin" && depth === 0).length, 1);
  const reference = new DatabaseSync(":memory:");
  try {
    reference.exec(batchSchema);
    for (const msg of messages) assert.equal(reference.applyChangeset(msg.changeset), true);
    assert.deepEqual(f.rows(), sqlArrays(reference));
  } finally { reference.close(); }
});

test("batch receiver: replay-only and mixed windows bind the actual message, never callback count", async t => {
  const f = await batchSetup(t), messages = [message(), message(2n), message(3n)];
  await f.receiver.receive(messages[0]);
  const mixed = await f.receiver.receiveBatch(messages);
  assert.deepEqual(mixed.map(r => r.replayed), [true, false, false]);
  assert.deepEqual(f.calls, messages.map(m => m.deliveryId)); assert.equal(f.confirms, 2);
  const replay = await f.receiver.receiveBatch(messages);
  assert.ok(replay.every(r => r.replayed));
  assert.deepEqual(replay.map(r => r.deliveryId), messages.map(m => m.deliveryId));
  assert.equal(f.calls.length, 3); assert.equal(f.confirms, 3);
  assert.equal((await f.order.head()).sequence, 3n);
});

test("batch receiver: late SQL failure rejects the whole fresh batch and permits exact retry", async t => {
  const f = await batchSetup(t), messages = [message(1n, "same"), message(2n, "same")];
  await assert.rejects(f.receiver.receiveBatch(messages), /UNIQUE/);
  assert.deepEqual(f.rows(), []); noBatchCommit(f);
  f.applicationOptions.constraintConflicts = "isolate";
  f.applicationOptions.onConflict = c => { assert.equal(c.kind, "constraint"); return "omit"; };
  const receipt = await f.receiver.receiveBatch(messages);
  assert.deepEqual(receipt.map(r => [r.applied, r.omitted]), [[1, 0], [0, 1]]);
  assert.equal(f.confirms, 1); assert.deepEqual(f.rows(), [[1n, "same"]]);
  // The unchanged sender transport can verify each ordinary receipt; no new ACK
  // format or implicit acceptance of omissions is smuggled into the protocol.
  for (let i = 0; i < messages.length; i++) {
    const transport = createOrderedChangesetTransport(sourceFor(messages[i]), transportOptions(async () => receipt[i]));
    assert.deepEqual(await transport(messages[i], {}), receipt[i]);
  }
});

const fkDDL = "CREATE TABLE parents(id INTEGER PRIMARY KEY);" +
  "CREATE TABLE children(id INTEGER PRIMARY KEY,pid INTEGER REFERENCES parents(id),value TEXT UNIQUE);" +
  "CREATE TABLE decisions(id TEXT PRIMARY KEY,info BLOB)";
function fkMessages() {
  return [
    nativeBatchMessage(1n, fkDDL, "INSERT INTO children VALUES(1,7,'new'),(2,7,'taken')"),
    nativeBatchMessage(2n, fkDDL, "INSERT INTO parents VALUES(7)"),
  ];
}
test("batch receiver: local whole-batch FK policy composes isolation and journal savepoints", async t => {
  const f = await batchSetup(t, { ddl: fkDDL, tables: ["children", "parents"], receiver: { batchForeignKeys: "defer" } });
  f.owner.db.exec("INSERT INTO parents VALUES(8); INSERT INTO children VALUES(8,8,'taken')");
  const messages = fkMessages();
  f.applicationOptions.constraintConflicts = "isolate";
  f.applicationOptions.onConflict = c => { assert.equal(c.kind, "constraint"); return "omit"; };
  f.applicationOptions.onRebase = (tx, info) => tx.transaction(child =>
    child.execute("INSERT INTO decisions VALUES(?,?)", [f.calls.at(-1), info]));
  const receipts = await f.receiver.receiveBatch(messages);
  assert.deepEqual(receipts.map(r => [r.applied, r.omitted]), [[1, 1], [1, 0]]);
  assert.deepEqual(f.rows("PRAGMA foreign_key_check"), []);
  assert.deepEqual(f.rows("SELECT * FROM children ORDER BY id"), [[1n, 7n, "new"], [8n, 8n, "taken"]]);
  assert.equal(f.rows("SELECT * FROM decisions").length, 2); assert.equal(f.confirms, 1);
  assert.deepEqual(f.rows("PRAGMA defer_foreign_keys"), [[0n]]);
});

test("batch receiver: missing later FK parent rolls back journal, inbox and sequence", async t => {
  const f = await batchSetup(t, { ddl: fkDDL, tables: ["children", "parents"], receiver: { batchForeignKeys: "defer" } });
  f.applicationOptions.onRebase = (tx, info) => tx.execute("INSERT INTO decisions VALUES(?,?)", [f.calls.at(-1), info]);
  const messages = [nativeBatchMessage(1n, fkDDL, "INSERT INTO children VALUES(1,7,'child')"),
    nativeBatchMessage(2n, fkDDL, "INSERT INTO parents VALUES(99)")];
  await assert.rejects(f.receiver.receiveBatch(messages), /foreign.key/i);
  noBatchCommit(f); assert.deepEqual(f.rows("SELECT * FROM children"), []);
  assert.deepEqual(f.rows("SELECT * FROM parents"), []); assert.deepEqual(f.rows("SELECT * FROM decisions"), []);
});

test("batch receiver: incoming policy fields cannot enable FK deferral", async t => {
  const f = await batchSetup(t, { ddl: fkDDL, tables: ["children", "parents"] });
  const messages = fkMessages();
  for (const msg of messages) { msg.foreignKeys = "defer"; msg.batchForeignKeys = "defer"; }
  await assert.rejects(f.receiver.receiveBatch(messages, { batchForeignKeys: "defer", foreignKeys: "defer" }), /FOREIGN KEY/);
  noBatchCommit(f); assert.deepEqual(f.rows("SELECT * FROM children"), []);
});

test("batch receiver: all copies and identities are captured before first await", async t => {
  const f = await batchSetup(t), messages = [message(), message(2n)];
  const expected = messages.map(fakeAck);
  const promise = f.receiver.receiveBatch(messages);
  for (const msg of messages) {
    msg.changeset.fill(0); structuredClone(msg.changeset.buffer, { transfer: [msg.changeset.buffer] });
    msg.deliveryId = "mutated"; msg.order.sequence = "99";
  }
  messages.reverse(); messages.length = 0;
  assert.deepEqual(await promise, expected);
  assert.deepEqual(f.rows(), [[1n, "body-1"], [2n, "body-2"]]);
});

test("batch receiver: aggregate and per-message byte limits remain exact and unshadowable", async t => {
  const messages = [message(), message(2n)], size = messages.reduce((n, m) => n + m.changeset.length, 0);
  const f = await batchSetup(t, { receiver: { maxBatchBytes: size - 1 } });
  let fakeSizeReads = 0;
  class HiddenSize extends Uint8Array { get byteLength() { fakeSizeReads++; return 0; } }
  messages[1].changeset = new HiddenSize(messages[1].changeset);
  await assert.rejects(f.receiver.receiveBatch(messages), code("INPUT"));
  assert.equal(fakeSizeReads, 0); assert.equal(f.owner.events.length, 0); noBatchCommit(f);
  const exact = await createOrderedChangesetReceiver(f.order, { apply: f.apply, confirmCommit: f.confirmCommit,
    maxBatchBytes: size, maxMessageBytes: Math.max(...messages.map(m => m.changeset.length)) });
  assert.equal((await exact.receiveBatch(messages)).length, 2);
  const tooSmall = await createOrderedChangesetReceiver(f.order, { apply: f.apply, confirmCommit: f.confirmCommit,
    maxBatchBytes: size, maxMessageBytes: messages[0].changeset.length - 1 });
  await assert.rejects(tooSmall.receiveBatch(messages), code("INPUT"));
  assert.equal(f.confirms, 1);
});

test("batch receiver: sparse arrays, element accessors, invalid count and foreign routes reject before SQL", async t => {
  const f = await batchSetup(t); let reads = 0;
  const accessor = [message()]; Object.defineProperty(accessor, "0", { get() { reads++; return message(); } });
  const badRoute = message(2n); badRoute.order.streamId = "untrusted";
  const duplicate = message(2n); duplicate.deliveryId = message().deliveryId;
  const iterable = { *[Symbol.iterator]() { reads++; yield message(); } };
  for (const messages of [[], new Array(1), new Array(257), accessor, iterable,
    [message(), message(3n)], [message(), badRoute], [message(), duplicate]]) {
    await assert.rejects(f.receiver.receiveBatch(messages));
    assert.equal(f.owner.events.length, 0);
  }
  assert.equal(reads, 0); assert.equal(f.calls.length, 0); noBatchCommit(f);
});

test("batch receiver: invalid last digest, malformed tail and compact format reject before any application", async t => {
  const f = await batchSetup(t), badHash = message(2n); badHash.sha256 = "0".repeat(64);
  const good = message(2n), truncated = batchEnvelope(2n, good.changeset.slice(0, -1));
  const compact = batchEnvelope(2n, encodePatchset([{ name: "notes", primaryKey: [1, 0], changes: [
    { operation: "insert", indirect: false, new: [2n, "compact"] },
  ] }]));
  for (const last of [badHash, truncated, compact]) {
    await assert.rejects(f.receiver.receiveBatch([message(), last]));
    assert.equal(f.owner.events.length, 0); assert.equal(f.calls.length, 0); assert.equal(f.confirms, 0);
  }
});

test("batch receiver: 256 empty full messages are bounded identities, not an empty batch", async t => {
  const f = await batchSetup(t, { receiver: { maxBatchBytes: 1 } });
  const messages = Array.from({ length: 256 }, (_, i) => batchEnvelope(BigInt(i + 1), new Uint8Array()));
  const receipts = await f.receiver.receiveBatch(messages);
  assert.equal(receipts.length, 256); assert.ok(receipts.every(r => r.applied === 0 && r.omitted === 0));
  assert.equal(f.confirms, 1); assert.equal((await f.order.head()).sequence, 256n);
});

test("batch receiver: retained-prefix failure preserves old rows and never invokes later callbacks", async t => {
  const f = await batchSetup(t);
  await f.receiver.receive(message()); f.calls.length = 0;
  const changed = message(1n, "different");
  await assert.rejects(f.receiver.receiveBatch([changed, message(2n)]), gateCode("REUSE"));
  assert.equal(f.calls.length, 0); assert.equal(f.confirms, 1);
  await assert.rejects(f.receiver.receiveBatch([message(), message(2n, "body-1")]), /UNIQUE/);
  assert.deepEqual(f.rows(), [[1n, "body-1"]]); assert.equal((await f.order.head()).sequence, 1n);
  assert.equal(f.confirms, 1);
});

test("batch receiver: failed COMMIT rolls back all provisional members and skips confirmation", async t => {
  const f = await batchSetup(t);
  f.owner.beforeRelease = depth => { if (depth === 0) throw new Error("batch commit failed"); };
  await assert.rejects(f.receiver.receiveBatch([message(), message(2n)]), /batch commit failed/);
  noBatchCommit(f); assert.deepEqual(f.rows(), []);
  f.owner.beforeRelease = null;
  assert.equal((await f.receiver.receiveBatch([message(), message(2n)])).length, 2);
});

test("batch receiver: lost COMMIT response recovers exact receipts without rerunning callbacks", async t => {
  const f = await batchSetup(t), messages = [message(), message(2n)];
  f.owner.afterCommit = () => { throw new Error("lost commit response"); };
  await assert.rejects(f.receiver.receiveBatch(messages), /lost commit response/);
  assert.equal(f.rows().length, 2); assert.equal(f.confirms, 0); assert.equal(f.calls.length, 2);
  f.owner.afterCommit = null;
  assert.ok((await f.receiver.receiveBatch(messages)).every(r => r.replayed));
  assert.equal(f.confirms, 1); assert.equal(f.calls.length, 2);
});

test("batch receiver: WAL reopen after lost confirmation replays every member and confirms once", async t => {
  const path = join(mkdtempSync(join(tmpdir(), "fsqlite-batch-receiver-")), "receiver.db");
  // A separate owner is closed here; the test retains files rather than deleting evidence.
  const first = new BatchReceiverOwner(path);
  first.db.exec("PRAGMA journal_mode=WAL");
  const order = new ChangesetOrder(first, ID); await order.initialize();
  let calls = 0; const messages = [message(), message(2n)];
  const receiver = await createOrderedChangesetReceiver(order, {
    apply: (target, msg, options) => { calls++; return applyChangeset(target, msg.changeset, {
      tables: ["notes"], deliveryId: msg.deliveryId, ...options,
    }); },
    confirmCommit: async () => { assert.equal(first.depth, 0); throw new Error("lost storage confirmation"); },
  });
  try {
    await assert.rejects(receiver.receiveBatch(messages), /lost storage confirmation/);
    assert.equal(sqlArrays(first.db).length, 2); assert.equal(calls, 2);
  } finally { first.close(); }
  const reopened = await batchSetup(t, { path, wal: true });
  const receipts = await reopened.receiver.receiveBatch(messages);
  assert.ok(receipts.every(r => r.replayed)); assert.equal(reopened.calls.length, 0);
  assert.equal(reopened.confirms, 1); assert.equal(reopened.rows().length, 2);
});

for (const ending of ["cancel", "timeout"]) test(`batch receiver: ${ending} waits for confirmation and returns no partial ACK`, async t => {
  const f = await batchSetup(t), started = pause(), release = pause(), controller = new AbortController();
  const messages = [message(), message(2n)];
  f.confirmHook = async () => { started.resolve(); await release.promise; };
  const operation = f.receiver.receiveBatch(messages,
    ending === "cancel" ? { signal: controller.signal } : { timeoutMs: 200 });
  let settled = false; void operation.then(() => { settled = true; }, () => { settled = true; });
  try {
    await Promise.race([started.promise, operation.then(() => assert.fail("confirmation was not held"))]);
    if (ending === "cancel") controller.abort(); else await new Promise(r => setTimeout(r, 230));
    await tick(); assert.equal(settled, false); assert.equal(f.owner.depth, 0);
    await assert.rejects(f.receiver.receive(message(3n)), code("BUSY"));
    await assert.rejects(f.receiver.receiveBatch(messages), code("BUSY"));
    release.resolve(); await assert.rejects(operation, code(ending === "cancel" ? "CANCELLED" : "TIMEOUT"));
    assert.equal(f.rows().length, 2); assert.equal((await f.order.head()).sequence, 2n);
    f.confirmHook = null;
    assert.ok((await f.receiver.receiveBatch(messages)).every(r => r.replayed)); assert.equal(f.confirms, 2);
  } finally { release.resolve(); await Promise.allSettled([operation]); }
});

test("batch receiver: a single receive holds the same admission gate through confirmation", async t => {
  const f = await batchSetup(t), started = pause(), release = pause();
  f.confirmHook = async () => { started.resolve(); await release.promise; };
  const operation = f.receiver.receive(message());
  try {
    await Promise.race([started.promise, operation]);
    await assert.rejects(f.receiver.receiveBatch([message(2n)]), code("BUSY"));
    release.resolve(); await operation;
    f.confirmHook = null; await f.receiver.receiveBatch([message(2n)]);
  } finally { release.resolve(); await Promise.allSettled([operation]); }
});

test("batch receiver: pre-cancellation and cancellation during held journal work do not advance order", async t => {
  const f = await batchSetup(t), aborted = new AbortController(); aborted.abort();
  await assert.rejects(f.receiver.receiveBatch([message()], { signal: aborted.signal }), code("CANCELLED"));
  assert.equal(f.owner.events.length, 0);
  const entered = pause(), release = pause(), controller = new AbortController();
  let child;
  f.applicationOptions.onRebase = tx => { child = tx.transaction(async inner => {
    await inner.execute("INSERT INTO decisions VALUES('held',X'01')");
  }); };
  f.owner.beforeSql = async (kind, sql) => {
    if (kind === "execute" && sql.includes("VALUES('held'")) { entered.resolve(); await release.promise; }
  };
  const operation = f.receiver.receiveBatch([message(), message(2n)], { signal: controller.signal });
  let settled = false; void operation.then(() => { settled = true; }, () => { settled = true; });
  try {
    await Promise.race([entered.promise, operation.then(() => assert.fail("no held journal"))]);
    controller.abort(); await tick(); assert.equal(settled, false);
    await assert.rejects(f.receiver.receive(message()), code("BUSY"));
    release.resolve(); await assert.rejects(operation, /cancel/i);
    await Promise.allSettled([child]); noBatchCommit(f);
    assert.deepEqual(f.rows(), []); assert.deepEqual(f.rows("SELECT * FROM decisions"), []);
    assert.equal(f.calls.length, 1, "next message must not start while a child is pending");
  } finally { release.resolve(); await Promise.allSettled([operation, child]); }
});

test("batch receiver: caught child settlement failure retains uncertainty and prevents confirmation", async t => {
  const f = await batchSetup(t), boundary = new Error("uncertain child release");
  f.owner.beforeRelease = depth => { if (depth === 1) throw boundary; };
  f.applicationOptions.onRebase = async tx => {
    await tx.transaction(child => child.execute("INSERT INTO decisions VALUES('failed',X'01')")).catch(() => {});
  };
  await assert.rejects(f.receiver.receiveBatch([message(), message(2n)]),
    error => error.cleanupErrors?.length > 0);
  assert.equal(f.calls.length, 1); noBatchCommit(f); assert.deepEqual(f.rows(), []);
});

test("batch receiver: malformed application results roll back the complete batch", async t => {
  const f = await batchSetup(t), messages = [message(), message(2n)];
  const lying = await createOrderedChangesetReceiver(f.order, {
    confirmCommit: f.confirmCommit,
    apply: async (target, msg, options) => {
      if (msg.deliveryId === messages[0].deliveryId) return f.apply(target, msg, options);
      // Both the returned SQL result and callback lie identically. The admitted
      // message count, not agreement between those two objects, must reject it.
      return target.transaction(async tx => {
        await tx.execute("INSERT INTO notes VALUES(2,'unreported')");
        return { applied: 0, omitted: 0, replayed: false };
      });
    },
  });
  await assert.rejects(lying.receiveBatch(messages), code("RECEIPT"));
  noBatchCommit(f); assert.deepEqual(f.rows(), []);
});

test("batch receiver: premature late callback waits for admitted SQL then rolls back earlier messages", async t => {
  const f = await batchSetup(t), started = pause(), release = pause();
  let admitted;
  const receiver = await createOrderedChangesetReceiver(f.order, {
    confirmCommit: f.confirmCommit,
    apply: async (target, msg, options) => {
      if (msg.deliveryId === message().deliveryId) return f.apply(target, msg, options);
      admitted = target.transaction(async tx => {
        await tx.execute("INSERT INTO notes VALUES(2,'late')");
        started.resolve(); await release.promise; return { applied: 1, omitted: 0, replayed: false };
      });
      return { applied: 1, omitted: 0, replayed: false };
    },
  });
  const operation = receiver.receiveBatch([message(), message(2n)]);
  let settled = false; void operation.then(() => { settled = true; }, () => { settled = true; });
  try {
    await Promise.race([started.promise, operation.then(() => assert.fail("no late work"))]);
    await tick(); assert.equal(settled, false);
    release.resolve(); await assert.rejects(operation, code("RECEIPT"));
    noBatchCommit(f); assert.deepEqual(f.rows(), []);
  } finally { release.resolve(); await Promise.allSettled([operation, admitted]); }
});

test("batch receiver: constructor captures local limits and policy before asynchronous opening", async t => {
  const f = await batchSetup(t), messages = [message(), message(2n)], size = messages.reduce((n, m) => n + m.changeset.length, 0);
  for (const bad of [{ maxBatchBytes: 0 }, { maxBatchBytes: 64 * 1024 * 1024 + 1 }, { batchForeignKeys: true }])
    await assert.rejects(createOrderedChangesetReceiver(f.order, { apply: f.apply, confirmCommit: f.confirmCommit, ...bad }), code("INPUT"));
  const config = { apply: f.apply, confirmCommit: f.confirmCommit, maxBatchBytes: size };
  const pending = createOrderedChangesetReceiver(f.order, config);
  config.maxBatchBytes = 1; config.batchForeignKeys = "invalid";
  config.apply = config.confirmCommit = () => assert.fail("mutable config was retained");
  const receiver = await pending;
  assert.equal((await receiver.receiveBatch(messages)).length, 2);
});
