// Actual production ledger/adapters on native SQLite. Not Rust/WASM certification.
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import test from "node:test";
import { ChangesetOrder } from "../src/changeset-order.ts";
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
// Batch delivery uses the production application, ledger, FK wrapper and owned
// child-savepoint path. SQL and fault hooks below never fabricate query results.
import * as orderedDelivery from "../src/changeset-ordered-delivery.ts";
import { applyChangeset, CHANGESET_RECEIPTS_TABLE } from "../src/changeset-apply.ts";
import { decodeRebaseInfo } from "../src/changeset-codec.ts";
import { FrankenSQLiteError } from "../src/errors.ts";
const appSchema = "CREATE TABLE IF NOT EXISTS notes(id INTEGER PRIMARY KEY,body TEXT UNIQUE);" +
  "CREATE TABLE IF NOT EXISTS decisions(id TEXT PRIMARY KEY,info BLOB);" +
  "CREATE TABLE IF NOT EXISTS parents(id INTEGER PRIMARY KEY);" +
  "CREATE TABLE IF NOT EXISTS children(id INTEGER PRIMARY KEY,parent INTEGER REFERENCES parents(id));";
class BatchOwner {
  constructor(path = ":memory:") {
    this.db = new DatabaseSync(path); this.db.exec("PRAGMA foreign_keys=ON;" + appSchema);
    this.depth = 0; this.serial = 0; this.events = []; this.beforeSql = null;
    this.beforeCommit = null; this.afterCommit = null; this.beforeRollback = null;
  }
  async transaction(work) {
    const depth = this.depth++, name = `delivery_${++this.serial}`;
    this.events.push(["begin", depth]); this.db.exec(depth ? `SAVEPOINT ${name}` : "BEGIN");
    let open = true, committed = false;
    const check = () => assert.ok(open, "expired test-owner executor");
    const tx = {
      execute: async (sql, params = []) => {
        check(); this.events.push(["execute", depth, sql]); await this.beforeSql?.("execute", sql, depth);
        try { return Number(this.db.prepare(sql).run(...params).changes); }
        catch (error) {
          if (error.code === "ERR_SQLITE_ERROR" && (error.errcode & 255) === 19)
            throw new FrankenSQLiteError({ code: "SQLITE_CONSTRAINT", sqliteCode: 19,
              extendedCode: error.errcode, message: error.message });
          throw error;
        }
      },
      query: async (sql, params = []) => {
        check(); this.events.push(["query", depth, sql]); await this.beforeSql?.("query", sql, depth);
        const s = this.db.prepare(sql); s.setReadBigInts(true); s.setReturnArrays(true);
        return { rowArrays: s.all(...params) };
      },
      transaction: child => { check(); return this.transaction(child); },
    };
    try {
      const result = await work(tx); open = false;
      await this.beforeCommit?.(depth);
      this.db.exec(depth ? `RELEASE ${name}` : "COMMIT"); committed = true;
      this.events.push(["commit", depth]);
      if (!depth) await this.afterCommit?.();
      return result;
    } catch (error) {
      open = false;
      if (!committed) {
        try {
          await this.beforeRollback?.(depth, error);
          this.db.exec(depth ? `ROLLBACK TO ${name}; RELEASE ${name}` : "ROLLBACK");
          this.events.push(["rollback", depth]);
        } catch (cleanup) {
          throw new AggregateError([error, cleanup], "owner rollback failed", { cause: error });
        }
      }
      throw error;
    } finally { open = false; this.depth--; }
  }
  rows(sql = "SELECT * FROM notes ORDER BY id") {
    const s = this.db.prepare(sql); s.setReadBigInts(true); s.setReturnArrays(true); return s.all();
  }
  close() { this.db.close(); }
}
function nativeEnvelope(sequence, table, values, ddl = appSchema) {
  const db = new DatabaseSync(":memory:"); db.exec(ddl);
  // The source is consistent; only the child table is captured into this
  // message. Its parent's separate message is deliberately delivered later.
  if (table === "children") db.prepare("INSERT INTO parents VALUES(?)").run(values[1]);
  const session = db.createSession({ table });
  try {
    db.prepare(`INSERT INTO ${table} VALUES(${values.map(() => '?').join(',')})`).run(...values);
    const changeset = session.changeset();
    return { protocol: "fsqlite-changeset-v1", receiverId: ID.receiverId,
      deliveryId: `native:${sequence}`, sha256: HASH(changeset), changeset,
      order: { protocol: CHANGESET_ORDER_PROTOCOL, streamId: ID.sourceId, sequence: String(sequence) } };
  } finally { session.close(); db.close(); }
}
async function batchSetup(t, receiverOptions = {}, owner = new BatchOwner()) {
  t.after(() => owner.close());
  const order = new ChangesetOrder(owner, ID); await order.initialize(); owner.events.length = 0;
  const state = { calls: [], confirms: 0, conflicts: 0 };
  const apply = (target, msg, controls) => {
    state.calls.push(msg.deliveryId);
    return applyChangeset(target, msg.changeset, { tables: ["notes", "parents", "children"],
      deliveryId: msg.deliveryId, ...controls, constraintConflicts: "isolate",
      onConflict: c => { state.conflicts++; return c.kind === "constraint" ? "omit" : "abort"; },
      onRebase: async (tx, bytes) => {
        await tx.transaction(async child => {
          await child.execute(`INSERT INTO decisions VALUES(?,${bytes.length ? '?' : 'zeroblob(0)'})`,
            bytes.length ? [msg.deliveryId, bytes] : [msg.deliveryId]);
        });
      } });
  };
  const confirmCommit = async () => { assert.equal(owner.depth, 0); state.confirms++; };
  const receiver = await createOrderedChangesetReceiver(order, { apply, confirmCommit, ...receiverOptions });
  owner.events.length = 0;
  return { owner, order, receiver, state, apply, confirmCommit };
}
function assertUnapplied(f) {
  assert.deepEqual(f.owner.rows(), []);
  assert.deepEqual(f.owner.rows("SELECT * FROM decisions"), []);
  const present = f.owner.rows(`SELECT 1 FROM sqlite_schema WHERE name='${CHANGESET_RECEIPTS_TABLE}'`);
  if (present.length) assert.deepEqual(f.owner.rows(`SELECT * FROM ${CHANGESET_RECEIPTS_TABLE}`), []);
  assert.equal(f.state.confirms, 0);
}
const turn = () => new Promise(resolve => setImmediate(resolve));

const transportOptions = deliver => ({ receiverId: ID.receiverId, streamId: ID.sourceId, deliver });
const fakeAck = msg => ({ protocol: msg.protocol, receiverId: msg.receiverId, deliveryId: msg.deliveryId,
  sha256: msg.sha256, byteLength: msg.changeset.length, applied: 1, omitted: 0,
  replayed: false, confirmed: true, order: { ...msg.order } });

// SQL-backed source port; not the persistent ChangesetOutbox class.
function batchSource(t, messages, sequences = messages.map(m => BigInt(m.order.sequence))) {
  const db = new DatabaseSync(":memory:"); t.after(() => db.close());
  db.exec("CREATE TABLE outgoing(seq INTEGER,id TEXT PRIMARY KEY,hash TEXT,payload BLOB,changes INTEGER,acked INTEGER)");
  for (const [i, m] of messages.entries()) db.prepare("INSERT INTO outgoing VALUES(?,?,?,?,1,0)")
    .run(sequences[i], m.deliveryId, m.sha256, m.changeset);
  const source = { receiverId: ID.receiverId, db, reads: [],
    async read(id) {
      this.reads.push(id); const statement = db.prepare("SELECT * FROM outgoing WHERE id=?"); statement.setReadBigInts(true);
      const r = statement.get(id); if (!r) return null;
      return { delivery: { sequence: r.seq, deliveryId: r.id, sha256: r.hash, byteLength: r.payload.length,
        changes: Number(r.changes), acknowledged: r.acked === 1n }, changeset: new Uint8Array(r.payload) };
    } };
  return source;
}

test("batch transport: retained source order reaches real receiver in one exchange with no implicit ACK", async t => {
  const f = await batchSetup(t), values = [message(), message(2n)], source = batchSource(t, values); let sends = 0;
  const transport = orderedDelivery.createOrderedChangesetBatchTransport(source, transportOptions((messages, controls) => {
    sends++; assert.ok(Object.isFrozen(messages)); assert.equal(source.reads.length, 2); assert.equal(f.owner.depth, 0);
    return f.receiver.receiveBatch(messages, controls);
  }));
  for (const m of values) m.order.sequence = "999";
  const receipts = await transport(values);
  assert.ok(Object.isFrozen(receipts)); assert.deepEqual(receipts.map(r => r.order.sequence), ["1", "2"]);
  assert.equal(sends, 1); assert.equal(f.state.confirms, 1);
  assert.equal(source.db.prepare("SELECT count(*) n FROM outgoing WHERE acked=0 AND length(payload)>0").get().n, 2);
});

test("batch transport: lost response retries the same window, with no rerun and fresh confirmation", async t => {
  const f = await batchSetup(t), values = [message(), message(2n)], source = batchSource(t, values); let lost = true;
  const transport = orderedDelivery.createOrderedChangesetBatchTransport(source, transportOptions(async (messages, controls) => {
    const receipts = await f.receiver.receiveBatch(messages, controls);
    if (lost) { lost = false; throw new Error("lost batch response"); } return receipts;
  }));
  await assert.rejects(transport(values), /lost batch response/);
  const replay = await transport(values);
  assert.ok(replay.every(r => r.replayed)); assert.equal(f.state.calls.length, 2); assert.equal(f.state.confirms, 2);
  assert.equal(source.db.prepare("SELECT count(*) n FROM outgoing WHERE acked=0").get().n, 2);
});

for (const bad of ["missing", "gap", "digest", "counts"]) test(`batch transport: ${bad} retained suffix prevents any transmission`, async t => {
  const values = [message(), message(2n)], source = batchSource(t, values); let sends = 0;
  if (bad === "missing") source.db.exec("DELETE FROM outgoing WHERE seq=2");
  if (bad === "gap") source.db.exec("UPDATE outgoing SET seq=3 WHERE seq=2");
  if (bad === "digest") source.db.exec("UPDATE outgoing SET hash='bad' WHERE seq=2");
  if (bad === "counts") source.db.exec("UPDATE outgoing SET changes=-1 WHERE seq=2");
  const transport = orderedDelivery.createOrderedChangesetBatchTransport(source, transportOptions(async messages => {
    sends++; return messages.map(fakeAck);
  }));
  await assert.rejects(transport(values)); assert.equal(sends, 0);
});

for (const bad of ["short", "extra", "reorder", "digest", "count", "unconfirmed", "accessor"]) test(`batch transport: ${bad} receipt suffix cannot release a valid prefix`, async t => {
  const values = [message(), message(2n)], source = batchSource(t, values); let getters = 0;
  const transport = orderedDelivery.createOrderedChangesetBatchTransport(source, transportOptions(async messages => {
    const receipts = messages.map(fakeAck);
    if (bad === "short") receipts.pop();
    if (bad === "extra") receipts.push(receipts[0]);
    if (bad === "reorder") receipts.reverse();
    if (bad === "digest") receipts[1].sha256 = "0".repeat(64);
    if (bad === "count") receipts[1].applied = 2;
    if (bad === "unconfirmed") receipts[1].confirmed = false;
    if (bad === "accessor") Object.defineProperty(receipts, "1", { get() { getters++; throw new Error("getter"); } });
    return receipts;
  }));
  await assert.rejects(transport(values)); assert.equal(getters, 0);
  assert.equal(source.db.prepare("SELECT count(*) n FROM outgoing WHERE acked=0 AND length(payload)>0").get().n, 2);
});

for (const forged of [false, true]) test(`batch transport: buffer transfer retains original receipt lengths (forged=${forged})`, async t => {
  const values = [message(), message(2n)], source = batchSource(t, values);
  const transport = orderedDelivery.createOrderedChangesetBatchTransport(source, transportOptions(async messages => {
    const receipts = messages.map(fakeAck);
    for (const m of messages) structuredClone(m.changeset.buffer, { transfer: [m.changeset.buffer] });
    if (forged) receipts[1].byteLength = 0;
    return receipts;
  }));
  if (forged) await assert.rejects(transport(values), code("RECEIPT"));
  else assert.deepEqual((await transport(values)).map(r => r.byteLength), values.map(m => m.changeset.length));
});

test("batch transport: input, source configuration and high int64 source sequences are captured exactly", async t => {
  const values = [message(), message(2n)], last = (1n << 63n) - 1n, source = batchSource(t, values, [last - 1n, last]);
  const opts = { ...transportOptions(async messages => JSON.parse(JSON.stringify(messages.map(fakeAck)))), maxBatchBytes: 1024 };
  const transport = orderedDelivery.createOrderedChangesetBatchTransport(source, opts);
  opts.deliver = () => assert.fail("replaced callback"); opts.maxBatchBytes = 1; source.read = opts.deliver;
  const pending = transport(values); for (const m of values) m.changeset.fill(0); values.reverse();
  const result = await pending; assert.deepEqual(result.map(r => r.order.sequence), [String(last - 1n), String(last)]);
});

for (const ending of ["cancel", "timeout"]) test(`batch transport: ${ending} drains active delivery before permitting another call`, async t => {
  const values = [message(), message(2n)], source = batchSource(t, values), entered = pause(), release = pause(), abort = new AbortController();
  const transport = orderedDelivery.createOrderedChangesetBatchTransport(source, transportOptions(async messages => {
    entered.resolve(); await release.promise; return messages.map(fakeAck);
  }));
  const operation = transport(values, ending === "cancel" ? { signal: abort.signal } : { timeoutMs: 100 });
  let settled = false; void operation.then(() => { settled = true; }, () => { settled = true; });
  try {
    await entered.promise; if (ending === "cancel") abort.abort(); else await new Promise(r => setTimeout(r, 120));
    await assert.rejects(transport(values), code("BUSY")); await turn(); assert.equal(settled, false);
    release.resolve(); await assert.rejects(operation, code(ending === "cancel" ? "CANCELLED" : "TIMEOUT"));
    assert.equal(source.db.prepare("SELECT count(*) n FROM outgoing WHERE acked=0").get().n, 2);
    assert.equal((await transport(values)).length, 2);
  } finally { release.resolve(); await Promise.allSettled([operation]); }
});

test("batch transport: exact aggregate input limit and sender omission policy remain explicit", async t => {
  const values = [message(), message(2n)], source = batchSource(t, values), total = values.reduce((n, m) => n + m.changeset.length, 0);
  let sends = 0;
  const opts = { ...transportOptions(async messages => { sends++; return messages.map(m => ({ ...fakeAck(m), applied: 0, omitted: 1 })); }),
    maxBatchBytes: total - 1 };
  await assert.rejects(orderedDelivery.createOrderedChangesetBatchTransport(source, opts)(values), code("INPUT"));
  assert.equal(source.reads.length, 0); assert.equal(sends, 0);
  const receipts = await orderedDelivery.createOrderedChangesetBatchTransport(source, { ...opts, maxBatchBytes: total })(values);
  assert.deepEqual(receipts.map(r => r.omitted), [1, 1]); assert.equal(sends, 1);
  assert.equal(source.db.prepare("SELECT count(*) n FROM outgoing WHERE acked=0").get().n, 2);
});

// Publication-time checks for the reconciled sender and independently landed receiver.
import { encodePatchset } from "../src/changeset-codec.ts";

for (const retainedCount of [0, 2]) test(`batch transport: valid but false source count ${retainedCount} rejects before remote commit`, async t => {
  const f = await batchSetup(t), values = [message(), message(2n)], source = batchSource(t, values);
  source.db.prepare("UPDATE outgoing SET changes=? WHERE seq=2").run(retainedCount);
  let sends = 0;
  const transport = orderedDelivery.createOrderedChangesetBatchTransport(source, transportOptions((messages, controls) => {
    sends++; return f.receiver.receiveBatch(messages, controls);
  }));
  await assert.rejects(transport(values), code("RECEIPT"));
  assert.equal(sends, 0); assert.equal(f.owner.events.length, 0);
  assert.equal((await f.order.head()).sequence, 0n);
  assert.equal(source.db.prepare("SELECT count(*) n FROM outgoing WHERE acked=0").get().n, 2);
});

test("batch transport: correctly hashed malformed or compact suffix rejects before source reads", async t => {
  const valid = message(2n), compact = encodePatchset([{ name: "notes", primaryKey: [1, 0], changes: [
    { operation: "insert", indirect: false, new: [2n, "compact"] },
  ] }]);
  for (const bytes of [valid.changeset.slice(0, -1), compact]) {
    const values = [message(), { ...valid, changeset: bytes, sha256: HASH(bytes) }];
    const source = batchSource(t, values); let sends = 0;
    const transport = orderedDelivery.createOrderedChangesetBatchTransport(source, transportOptions(async messages => {
      sends++; return messages.map(fakeAck);
    }));
    await assert.rejects(transport(values), { code: "ERR_FSQLITE_CHANGESET_FORMAT" });
    assert.equal(source.reads.length, 0); assert.equal(sends, 0);
  }
});

test("batch transport: pre-cancellation never reads payload accessors or enters source", async () => {
  const abort = new AbortController(); abort.abort(); let calls = 0;
  const values = [message()];
  Object.defineProperty(values, "0", { get() { calls++; throw new Error("payload getter"); } });
  const transport = orderedDelivery.createOrderedChangesetBatchTransport({ read: async () => { calls++; } },
    transportOptions(async () => { calls++; }));
  await assert.rejects(transport(values, { signal: abort.signal }), code("CANCELLED"));
  assert.equal(calls, 0);
});

test("batch transport: cancellation joins an admitted source read and sends no partial window", async t => {
  const values = [message(), message(2n)], source = batchSource(t, values);
  const entered = pause(), release = pause(), abort = new AbortController(); let sends = 0, settled = false;
  const read = source.read;
  source.read = async function(id) { entered.resolve(); await release.promise; return read.call(this, id); };
  const transport = orderedDelivery.createOrderedChangesetBatchTransport(source, transportOptions(async messages => {
    sends++; return messages.map(fakeAck);
  }));
  const operation = transport(values, { signal: abort.signal });
  void operation.then(() => { settled = true; }, () => { settled = true; });
  try {
    await entered.promise; abort.abort(); await turn(); assert.equal(settled, false);
    await assert.rejects(transport(values), code("BUSY"));
    release.resolve(); await assert.rejects(operation, code("CANCELLED"));
    assert.equal(source.reads.length, 1); assert.equal(sends, 0);
    assert.equal((await transport(values)).length, 2);
  } finally { release.resolve(); await Promise.allSettled([operation]); }
});

test("batch transport: retained source and receipt field accessors never execute", async t => {
  const values = [message(), message(2n)]; let getters = 0, sends = 0;
  const source = batchSource(t, values), read = source.read;
  source.read = async function(id) {
    const record = await read.call(this, id);
    if (id === values[1].deliveryId)
      Object.defineProperty(record.delivery, "sequence", { get() { getters++; return 2n; } });
    return record;
  };
  const blocked = orderedDelivery.createOrderedChangesetBatchTransport(source, transportOptions(async messages => {
    sends++; return messages.map(fakeAck);
  }));
  await assert.rejects(blocked(values), code("INPUT")); assert.equal(getters, 0); assert.equal(sends, 0);
  source.read = read;
  const transport = orderedDelivery.createOrderedChangesetBatchTransport(source, transportOptions(async messages => {
    const receipts = messages.map(fakeAck);
    Object.defineProperty(receipts[1], "confirmed", { get() { getters++; return true; } });
    return receipts;
  }));
  await assert.rejects(transport(values), code("INPUT")); assert.equal(getters, 0);
});

test("batch transport: batch shape and typed storage reject before source or delivery", async t => {
  const first = message(), second = message(2n), source = batchSource(t, [first, second]);
  let calls = 0;
  const accessor = [first]; Object.defineProperty(accessor, "0", { get() { calls++; return first; } });
  const detached = new Uint8Array(second.changeset);
  structuredClone(detached.buffer, { transfer: [detached.buffer] });
  const transport = orderedDelivery.createOrderedChangesetBatchTransport(source, transportOptions(async messages => {
    calls++; return messages.map(fakeAck);
  }));
  for (const values of [[], new Array(1), new Array(257), accessor, [first, first],
    [first, { ...second, changeset: detached }],
    [first, { ...second, changeset: new Uint8Array(new SharedArrayBuffer(1)) }],
    [first, { ...second, changeset: new Uint8Array(new ArrayBuffer(1, { maxByteLength: 2 })) }]]) {
    await assert.rejects(transport(values));
    assert.equal(source.reads.length, 0); assert.equal(calls, 0);
  }
});

test("batch transport: 256 native messages preserve one confirmed exchange and every source identity", async t => {
  const values = Array.from({ length: 256 }, (_, index) => message(BigInt(index + 1)));
  const f = await batchSetup(t), source = batchSource(t, values); let sends = 0;
  const transport = orderedDelivery.createOrderedChangesetBatchTransport(source, transportOptions((messages, controls) => {
    sends++; return f.receiver.receiveBatch(messages, controls);
  }));
  const receipts = await transport(values);
  assert.equal(receipts.length, 256); assert.equal(receipts.at(-1).order.sequence, "256");
  assert.equal(f.state.confirms, 1); assert.equal(sends, 1); assert.equal(f.owner.rows().length, 256);
  assert.equal(source.db.prepare("SELECT count(*) n FROM outgoing WHERE acked=0").get().n, 256);
});

test("batch transport: cross-message parents and nested journal decisions commit before confirmation", async t => {
  const f = await batchSetup(t, { batchForeignKeys: "defer" });
  const values = [nativeEnvelope(1n, "children", [1n, 7n]), nativeEnvelope(2n, "parents", [7n])];
  const source = batchSource(t, values);
  const transport = orderedDelivery.createOrderedChangesetBatchTransport(source, transportOptions((messages, controls) =>
    f.receiver.receiveBatch(messages, controls)));
  const receipts = await transport(values);
  assert.deepEqual(receipts.map(r => [r.applied, r.omitted]), [[1, 0], [1, 0]]);
  assert.deepEqual(f.owner.rows("SELECT * FROM children"), [[1n, 7n]]);
  assert.deepEqual(f.owner.rows("PRAGMA foreign_key_check"), []);
  assert.equal(f.owner.rows("SELECT * FROM decisions").length, 2);
  assert.equal(f.state.confirms, 1);
  assert.equal(source.db.prepare("SELECT count(*) n FROM outgoing WHERE acked=0").get().n, 2);
});

test("batch transport: cancellation during real receiver confirmation retains replayable source payloads", async t => {
  const entered = pause(), release = pause(), abort = new AbortController(); let barriers = 0;
  const f = await batchSetup(t, { confirmCommit: async () => {
    assert.equal(f.owner.depth, 0); barriers++; entered.resolve(); await release.promise;
  } });
  const values = [message(), message(2n)], source = batchSource(t, values);
  const transport = orderedDelivery.createOrderedChangesetBatchTransport(source, transportOptions((messages, controls) =>
    f.receiver.receiveBatch(messages, controls)));
  const operation = transport(values, { signal: abort.signal }); let settled = false;
  void operation.then(() => { settled = true; }, () => { settled = true; });
  try {
    await entered.promise; abort.abort(); await turn(); assert.equal(settled, false);
    assert.equal(f.owner.rows().length, 2); await assert.rejects(transport(values), code("BUSY"));
    release.resolve(); await assert.rejects(operation, code("CANCELLED"));
    assert.equal(source.db.prepare("SELECT count(*) n FROM outgoing WHERE acked=0 AND length(payload)>0").get().n, 2);
    const recovered = await transport(values);
    assert.ok(recovered.every(r => r.replayed)); assert.equal(barriers, 2); assert.equal(f.state.calls.length, 2);
  } finally { release.resolve(); await Promise.allSettled([operation]); }
});
