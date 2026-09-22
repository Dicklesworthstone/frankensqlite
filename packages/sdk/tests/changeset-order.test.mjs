// Production order-admission SQL on reference SQLite, not Rust/WASM certification.
import assert from "node:assert/strict";
import { test } from "node:test";
import { DatabaseSync } from "node:sqlite";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createHash } from "node:crypto";
import { ChangesetOrder, CHANGESET_ORDER_TABLE } from "../src/changeset-order.ts";

const T = `"${CHANGESET_ORDER_TABLE}"`;
const HASH = bytes => createHash("sha256").update(bytes).digest("hex");
function message(sequence = 1n, value = "first", deliveryId = `source/${sequence}`) {
  const changeset = new TextEncoder().encode(value);
  return { sequence, deliveryId, sha256: HASH(changeset), changeset };
}
class SqliteTarget {
  constructor(path = ":memory:") {
    this.db = new DatabaseSync(path);
    this.db.exec("PRAGMA foreign_keys=ON; CREATE TABLE IF NOT EXISTS items(id INTEGER PRIMARY KEY, value TEXT NOT NULL)");
    this.transactions = 0;
  }
  executor() {
    return {
      execute: async (sql, params = []) => Number(this.db.prepare(sql).run(...params).changes),
      query: async (sql, params = []) => ({ rowArrays: this.db.prepare(sql).all(...params).map(row => Object.values(row)) }),
    };
  }
  async transaction(work) {
    this.transactions++;
    this.db.exec("BEGIN");
    try {
      const value = await work(this.executor());
      this.db.exec("COMMIT");
      return value;
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }
  rows() { return this.db.prepare("SELECT id,value FROM items ORDER BY id").all().map(row => Object.values(row)); }
  close() { this.db.close(); }
}
const opts = { receiverId: "replica/a", sourceId: "source/generation-1" };
async function setup(config = {}) {
  const target = new SqliteTarget();
  const order = new ChangesetOrder(target, { ...opts, ...config });
  await order.initialize();
  return { target, order };
}
function writer(id, beforeCommit = async () => {}) {
  return (scoped, bytes) => scoped.transaction(async tx => {
    await tx.execute("INSERT INTO items VALUES (?,?)", [BigInt(id), new TextDecoder().decode(bytes)]);
    await beforeCommit(tx, bytes);
    return { applied: 1, omitted: 0, replayed: false };
  });
}
const code = expected => error => error?.code === `ERR_FSQLITE_ORDER_${expected}`;

test("explicit enrollment; no automatic state creation during delivery", async () => {
  const target = new SqliteTarget(), order = new ChangesetOrder(target, opts);
  await assert.rejects(order.apply(message(), writer(1)), code("UNINITIALIZED"));
  assert.deepEqual(target.rows(), []);
  assert.equal((await order.initialize()).sequence, 0n);
  assert.equal((await order.initialize()).sequence, 0n);
  target.close();
});
test("reject reordered writes before application; recover by delivering the missing predecessor", async () => {
  const { target, order } = await setup();
  let calls = 0;
  await assert.rejects(order.apply(message(2n), async () => { calls++; return { applied: 0, omitted: 0, replayed: false }; }), code("GAP"));
  assert.equal(calls, 0);
  await order.apply(message(), writer(1));
  await order.apply(message(2n, "second"), writer(2));
  assert.deepEqual(target.rows(), [[1, "first"], [2, "second"]]);
  assert.equal((await order.head()).sequence, 2n);
  target.close();
});
test("old delivery retry returns original decisions without invoking callbacks", async () => {
  const { target, order } = await setup();
  await order.apply(message(), writer(1));
  await order.apply(message(2n, "second"), writer(2));
  const replay = await order.apply(message(), () => { throw new Error("must not run"); });
  assert.equal(replay.replayed, true);
  assert.equal(replay.applied, 1);
  assert.equal(replay.sequence, 1n);
  assert.equal((await order.head()).sequence, 2n);
  target.close();
});
for (const changed of [message(1n, "different"), message(1n, "first", "another-id"), message(2n, "first", "source/1")]) {
  test(`sequence/identity reuse is refused (${changed.sequence}/${changed.deliveryId}/${new TextDecoder().decode(changed.changeset)})`, async () => {
    const { target, order } = await setup();
    await order.apply(message(), writer(1));
    await assert.rejects(order.apply(changed, writer(2)), code("REUSE"));
    assert.deepEqual(target.rows(), [[1, "first"]]);
    target.close();
  });
}
test("lost ACK and file reopen retain exact admission and replay state", async () => {
  const path = join(mkdtempSync(join(tmpdir(), "fsqlite-order-")), "receiver.db");
  let target = new SqliteTarget(path), order = new ChangesetOrder(target, opts);
  await order.initialize();
  await order.apply(message(), writer(1));
  target.close();
  target = new SqliteTarget(path); order = new ChangesetOrder(target, opts);
  assert.equal((await order.initialize()).sequence, 1n);
  assert.equal((await order.apply(message(), () => { throw new Error("replayed writes"); })).replayed, true);
  await order.apply(message(2n, "second"), writer(2));
  assert.deepEqual(target.rows(), [[1, "first"], [2, "second"]]);
  target.close();
});
test("receiver and source bindings survive reconstruction of the owner", async () => {
  const { target, order } = await setup();
  await order.apply(message(), writer(1));
  for (const override of [{ sourceId: "other-source" }, { receiverId: "other-receiver" }]) {
    const wrong = new ChangesetOrder(target, { ...opts, ...override });
    await assert.rejects(wrong.initialize(), code("BINDING"));
    await assert.rejects(wrong.apply(message(2n), writer(2)), code("BINDING"));
  }
  assert.deepEqual(target.rows(), [[1, "first"]]);
  target.close();
});
test("row and side-effect journal failures roll back the sequence marker and all SQL", async () => {
  const { target, order } = await setup();
  target.db.exec("CREATE TABLE proof(value TEXT)");
  await assert.rejects(order.apply(message(), writer(1, async tx => {
    await tx.execute("INSERT INTO proof VALUES ('journal')");
    throw new Error("policy failure");
  })), /policy failure/);
  assert.deepEqual(target.rows(), []);
  assert.equal(target.db.prepare("SELECT count(*) AS n FROM proof").get().n, 0);
  assert.equal((await order.head()).sequence, 0n);
  await order.apply(message(), writer(1));
  target.close();
});
test("deferred COMMIT failure rolls back data AND ordering; same delivery can retry", async () => {
  const { target, order } = await setup();
  target.db.exec("CREATE TABLE parent(id INTEGER PRIMARY KEY); CREATE TABLE child(id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)");
  await assert.rejects(order.apply(message(), writer(1, tx => tx.execute("INSERT INTO child VALUES(1)"))), /FOREIGN KEY/);
  assert.deepEqual(target.rows(), []);
  assert.equal((await order.head()).sequence, 0n);
  target.db.exec("INSERT INTO parent VALUES(1)");
  await order.apply(message(), writer(1, tx => tx.execute("INSERT INTO child VALUES(1)")));
  assert.equal((await order.head()).sequence, 1n);
  target.close();
});
test("an unordered receipt cannot be retrospectively blessed into the stream", async () => {
  const { target, order } = await setup();
  await assert.rejects(order.apply(message(), async scoped => scoped.transaction(async tx => {
    await tx.execute("INSERT INTO items VALUES(1,'bad')");
    return { applied: 1, omitted: 0, replayed: true };
  })), code("REUSE"));
  assert.deepEqual(target.rows(), []);
  assert.equal((await order.head()).sequence, 0n);
  target.close();
});
test("cancellation before SQL and inside the callback never advances order", async () => {
  const { target, order } = await setup();
  const controller = new AbortController(); controller.abort();
  const before = target.transactions;
  await assert.rejects(order.apply(message(), writer(1), { signal: controller.signal }), code("CANCELLED"));
  assert.equal(target.transactions, before);
  const active = new AbortController();
  await assert.rejects(order.apply(message(), writer(1, async () => active.abort()), { signal: active.signal }), code("CANCELLED"));
  assert.deepEqual(target.rows(), []);
  assert.equal((await order.head()).sequence, 0n);
  target.close();
});
test("deadline covers application and is checked before commit", async () => {
  const { target, order } = await setup();
  await assert.rejects(order.apply(message(), writer(1, () => new Promise(resolve => setTimeout(resolve, 30))), { timeoutMs: 15 }), code("TIMEOUT"));
  assert.deepEqual(target.rows(), []);
  assert.equal((await order.head()).sequence, 0n);
  target.close();
});
test("retention limit refuses new work but still acknowledges exact retries", async () => {
  const { target, order } = await setup({ maxEntries: 1 });
  await order.apply(message(), writer(1));
  await assert.rejects(order.apply(message(2n), writer(2)), code("FULL"));
  assert.equal((await order.apply(message(), writer(2))).replayed, true);
  target.close();
});
test("payload is copied before asynchronous hashing and caller mutation", async () => {
  const { target, order } = await setup();
  const input = message();
  const pending = order.apply(input, writer(1));
  input.changeset.fill(0); input.deliveryId = "mutated";
  const saved = await pending;
  assert.equal(saved.deliveryId, "source/1");
  assert.deepEqual(target.rows(), [[1, "first"]]);
  target.close();
});
test("bad digest and oversized message are refused before entering SQL", async () => {
  const { target, order } = await setup({ maxMessageBytes: 5 });
  const before = target.transactions;
  await assert.rejects(order.apply({ ...message(), sha256: "0".repeat(64) }, writer(1)), code("INPUT"));
  await assert.rejects(order.apply(message(1n, "123456"), writer(1)), code("INPUT"));
  assert.equal(target.transactions, before);
  target.close();
});
test("shared, detached, resizable and accessor-controlled input is refused", async () => {
  const { target, order } = await setup();
  const detached = new Uint8Array([1]); structuredClone(detached.buffer, { transfer: [detached.buffer] });
  const arrays = [detached, new Uint8Array(new SharedArrayBuffer(1)), new Uint8Array(new ArrayBuffer(1, { maxByteLength: 10 }))];
  for (const changeset of arrays) await assert.rejects(order.apply({ ...message(), changeset }, writer(1)), code("INPUT"));
  let called = false;
  const input = message(); Object.defineProperty(input, "sequence", { get() { called = true; return 1n; } });
  await assert.rejects(order.apply(input, writer(1)), code("INPUT"));
  assert.equal(called, false);
  target.close();
});
test("missing middle receipt, binding row, or corrupted receipt fails closed", async () => {
  for (const sql of [`DELETE FROM ${T} WHERE seq=0`, `DELETE FROM ${T} WHERE seq=1`, `UPDATE ${T} SET sha256='bad' WHERE seq=2`]) {
    const { target, order } = await setup();
    await order.apply(message(), writer(1)); await order.apply(message(2n), writer(2));
    target.db.exec(sql);
    await assert.rejects(order.apply(message(3n), writer(3)), code("CORRUPT"));
    assert.equal(target.rows().length, 2);
    target.close();
  }
});
test("truncated last receipt cannot turn committed work into fresh work after reopen", async () => {
  const { target, order } = await setup();
  await order.apply(message(), writer(1));
  target.db.exec(`DELETE FROM ${T} WHERE seq=1`);
  const reopened = new ChangesetOrder(target, opts);
  await assert.rejects(reopened.apply(message(), writer(2)), code("CORRUPT"));
  assert.deepEqual(target.rows(), [[1, "first"]]);
  target.close();
});
test("application cannot mutate its admitted payload and retain the old digest", async () => {
  const { target, order } = await setup();
  await assert.rejects(order.apply(message(), writer(1, async (_tx, bytes) => bytes.fill(0))), code("CORRUPT"));
  assert.deepEqual(target.rows(), []);
  target.close();
});
test("application cannot tamper with the ledger or introduce metadata triggers", async () => {
  for (const sql of [`DELETE FROM ${T}`, `CREATE TRIGGER bad AFTER INSERT ON ${T} BEGIN DELETE FROM items; END`]) {
    const { target, order } = await setup();
    await assert.rejects(order.apply(message(), writer(1, tx => tx.execute(sql))));
    assert.deepEqual(target.rows(), []);
    assert.equal((await order.head()).sequence, 0n);
    target.close();
  }
});
test("concurrent calls on one owner are rejected, not silently queued or globally serialized", async () => {
  const { target, order } = await setup();
  let release, entered;
  const started = new Promise(resolve => { entered = resolve; });
  const gate = new Promise(resolve => { release = resolve; });
  const pending = order.apply(message(), writer(1, async () => { entered(); await gate; }));
  await started;
  await assert.rejects(order.apply(message(2n), writer(2)), code("BUSY"));
  release(); await pending;
  await order.apply(message(2n), writer(2));
  target.close();
});
test("escaped application target closes when the callback ends", async () => {
  const { target, order } = await setup();
  let escaped;
  await order.apply(message(), async (scoped, bytes) => { escaped = scoped; return writer(1)(scoped, bytes); });
  await assert.rejects(escaped.transaction(tx => tx.execute("INSERT INTO items VALUES(2,'late')")), code("INPUT"));
  assert.deepEqual(target.rows(), [[1, "first"]]);
  target.close();
});
test("unawaited rejected application work aborts instead of escaping past COMMIT", async () => {
  const { target, order } = await setup();
  await assert.rejects(order.apply(message(), async scoped => {
    void scoped.transaction(async tx => {
      await new Promise(resolve => setTimeout(resolve, 10));
      await tx.execute("INSERT INTO items VALUES(1,'late')");
      throw new Error("unobserved child failure");
    }).catch(() => {});
    return { applied: 1, omitted: 0, replayed: false };
  }), /unobserved child failure/);
  assert.deepEqual(target.rows(), []);
  target.close();
});
