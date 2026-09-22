import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { fileURLToPath } from "node:url";
import { test } from "node:test";
import { decodeChangeset, decodePatchset, decodeRebaseInfo, encodeChangeset, encodePatchset, encodeRebaseInfo } from "../src/changeset-codec.ts";
import { ChangesetRebaser, createChangesetRebaseInfo, rebaseChangeset } from "../src/changeset-rebase.ts";
import { applyChangeset, applyPatchset } from "../src/changeset-apply.ts";

const oraclePath = fileURLToPath(new URL("./helpers/rebase-oracle.py", import.meta.url));
function oracle(input) {
  const result = spawnSync(process.env.PYTHON ?? "python3", [oraclePath], {
    input: JSON.stringify(input), encoding: "utf8", maxBuffer: 16 * 1024 * 1024,
    timeout: 30000,
  });
  assert.equal(result.status, 0, `Native SQLite oracle failed: ${result.error ?? result.stderr}`);
  return JSON.parse(result.stdout);
}
const bytes = (hex) => Uint8Array.from(Buffer.from(hex, "hex"));
const hex = (data) => Buffer.from(data).toString("hex");
function value(v) {
  if (v === undefined) return { undefined: true };
  if (v === null) return { null: true };
  if (typeof v === "bigint") return { integer: String(v) };
  if (typeof v === "number") {
    const bits = Buffer.alloc(8); bits.writeDoubleBE(v); return { real: hex(bits) };
  }
  if (typeof v === "string") return { text: v };
  return { blob: hex(v) };
}
function records(wire) {
  return decodeChangeset(wire).flatMap((table) => table.changes.map((change) => ({
    name: table.name, primaryKey: table.primaryKey, operation: change.operation,
    indirect: change.indirect,
    ...(change.operation !== "insert" ? { old: change.old.map(value) } : {}),
    ...(change.operation !== "delete" ? { new: change.new.map(value) } : {}),
  })));
}
const schema = "CREATE TABLE t(k PRIMARY KEY,v,w);";
const seed = "INSERT INTO t VALUES(1,'a','b');";
const cases = [];
function scenario(name, local, remote, initial = seed, ddl = schema, indirect = false) {
  cases.push({ name, schema: ddl, seed: initial, local, remote, indirect });
}
for (const policy of ["omit", "replace"]) {
  scenario(`insert/insert ${policy}`, "INSERT INTO t VALUES(1,'local','l');",
    [{ sql: "INSERT INTO t VALUES(1,'remote','r');", policy }], "");
  scenario(`update/update ${policy}`, "UPDATE t SET v='local',w='local-w';",
    [{ sql: "UPDATE t SET v='remote';", policy }]);
  scenario(`update/delete ${policy}`, "UPDATE t SET v='local';",
    [{ sql: "DELETE FROM t;", policy }]);
  scenario(`all replaced ${policy}`, "UPDATE t SET v='local';",
    [{ sql: "UPDATE t SET v='remote';", policy }]);
  scenario(`no-op value ${policy}`, "UPDATE t SET v='same';",
    [{ sql: "UPDATE t SET v='same';", policy }]);
  scenario(`indirect insert ${policy}`, "INSERT INTO t VALUES(1,'local','l');",
    [{ sql: "INSERT INTO t VALUES(1,'remote','r');", policy }], "", schema, true);
}
scenario("delete/update omit", "DELETE FROM t;", [{ sql: "UPDATE t SET v='remote';", policy: "omit" }]);
scenario("delete/delete omit", "DELETE FROM t;", [{ sql: "DELETE FROM t;", policy: "omit" }]);
scenario("disjoint columns do not fabricate conflicts", "UPDATE t SET v='local';",
  [{ sql: "UPDATE t SET w='remote';", policy: "omit" }]);
scenario("unrelated key is unchanged", "UPDATE t SET v='local';",
  [{ sql: "INSERT INTO t VALUES(2,'remote','r');", policy: "omit" }]);
scenario("omitted remote delete resurrects full local row", "UPDATE t SET w=NULL;",
  [{ sql: "DELETE FROM t;", policy: "omit" }]);
scenario("replacement tombstone survives later omission", "UPDATE t SET v='local',w='local-w';", [
  { sql: "UPDATE t SET v='remote';", policy: "replace" },
  { sql: "UPDATE t SET v='new-remote',w='remote-w';", policy: "omit" },
]);
scenario("last omission rebases each updated field", "UPDATE t SET v='local',w='local-w';", [
  { sql: "UPDATE t SET v='r1';", policy: "omit" },
  { sql: "UPDATE t SET w='r2';", policy: "omit" },
  { sql: "UPDATE t SET v='r3';", policy: "omit" },
]);
scenario("replace then delete retains native precedence", "UPDATE t SET v='local',w='local-w';", [
  { sql: "UPDATE t SET v='remote';", policy: "replace" },
  { sql: "DELETE FROM t;", policy: "omit" },
]);
scenario("delete/reinsert omission", "UPDATE t SET v='local';", [
  { sql: "DELETE FROM t;", policy: "omit" },
  { sql: "INSERT INTO t VALUES(1,'remote','remote-w');", policy: "omit" },
]);
scenario("delete/reinsert replacement", "UPDATE t SET v='local';", [
  { sql: "DELETE FROM t;", policy: "omit" },
  { sql: "INSERT INTO t VALUES(1,'remote','remote-w');", policy: "replace" },
]);
scenario("insert then remote insert/delete", "INSERT INTO t VALUES(1,'local','l');", [
  { sql: "INSERT INTO t VALUES(1,'remote','r');", policy: "omit" },
  { sql: "DELETE FROM t;", policy: "omit" },
], "");
scenario("delete then update/delete", "DELETE FROM t;", [
  { sql: "UPDATE t SET v='remote';", policy: "omit" },
  { sql: "DELETE FROM t;", policy: "omit" },
]);
scenario("composite WITHOUT ROWID + exact blob key", "UPDATE t SET v=x'0100ff',w=9223372036854775807;", [
  { sql: "UPDATE t SET v=x'0200fe',w=-9223372036854775808;", policy: "omit" },
], "INSERT INTO t VALUES(x'0061','😀',NULL,0.5);",
"CREATE TABLE t(k BLOB,q TEXT,v,w,PRIMARY KEY(q,k)) WITHOUT ROWID;");
scenario("NUL text and Unicode values", "UPDATE t SET v='λ' || char(0) || '😀';", [
  { sql: "UPDATE t SET v='π' || char(0) || '猫';", policy: "omit" },
]);
for (let n = 0; n < 48; n++) {
  // Deterministic sequences with native-generated multi-key session ordering.
  const first = n % 3 + 1, second = (n + 1) % 3 + 1;
  scenario(`native workload ${n}`, `UPDATE t SET v='local${n}',w=${n}.5 WHERE k=${first}; DELETE FROM t WHERE k=${second};`, [
    { sql: `UPDATE t SET v='remote${n}' WHERE k=${first}; UPDATE t SET w=${n + 10}.5 WHERE k=${second};`, policy: "omit" },
    { sql: `UPDATE t SET v='remote-final${n}',w=${n + 20}.5 WHERE k=${first};`, policy: n % 2 ? "replace" : "omit" },
  ], "INSERT INTO t VALUES(1,'a','b'),(2,'c','d'),(3,'e','f');");
}
const generated = oracle(cases);
console.log(`Native SQLite session/rebaser oracle ${generated.sqlite}; ${generated.cases.length} workloads`);
const cross = oracle({ rebases: generated.cases.map((c) => ({
  local: c.localWire,
  buffers: c.remoteWires.map((wire, i) => hex(createChangesetRebaseInfo(
    bytes(wire), c.decisions[i].map((d) => ({ changeIndex: d.changeIndex, resolution: d.action })),
  ))),
})) });
for (const [caseIndex, c] of generated.cases.entries()) {
  test(`native oracle: ${c.name}`, () => {
    const rb = new ChangesetRebaser();
    for (let i = 0; i < c.rebaseBuffers.length; i++) {
      const produced = createChangesetRebaseInfo(bytes(c.remoteWires[i]), c.decisions[i].map((d) => ({
        changeIndex: d.changeIndex, resolution: d.action,
      })));
      assert.deepEqual(decodeRebaseInfo(produced), decodeRebaseInfo(bytes(c.rebaseBuffers[i])), "SDK and native decisions");
      rb.configure(bytes(c.rebaseBuffers[i]));
    }
    const output = rb.rebase(bytes(c.localWire));
    assert.deepEqual(records(output), c.expectedRecords);
    assert.deepEqual(records(rebaseChangeset(bytes(c.localWire), c.rebaseBuffers.map(bytes))), c.expectedRecords);
    // Run the SDK's generated configuration through the REAL C rebaser, too.
    assert.deepEqual(cross[caseIndex].records, c.expectedRecords);
  });
}
const first = generated.cases[0];
const rbInfo = () => bytes(first.rebaseBuffers[0]);
const local = () => bytes(first.localWire);
const rbTable = (key = 1n, name = "t", pk = [1, 0, 0]) => ({
  name, primaryKey: pk,
  changes: [{ operation: "insert", replace: false, values: [key, "r", "s"] }],
});
const info = (key = 1n) => encodeRebaseInfo([rbTable(key)]);

test("empty configurations and clear leave no stale decisions", () => {
  const rb = new ChangesetRebaser();
  assert.deepEqual(rb.stats(), { tables: 0, changes: 0, cells: 0, byteLength: 0 });
  rb.configure(new Uint8Array()); assert.deepEqual(rb.rebase(local()), local());
  rb.configure(rbInfo()); assert.notDeepEqual(rb.rebase(local()), local());
  rb.clear(); assert.deepEqual(rb.rebase(local()), local());
  assert.equal(rb.stats().changes, 0);
});
test("owned configuration and fresh output do not alias caller buffers", () => {
  const input = rbInfo(), rb = new ChangesetRebaser();
  rb.configure(input); input.fill(0);
  const before = rb.rebase(local()), altered = rb.rebase(local());
  altered.fill(0); assert.deepEqual(rb.rebase(local()), before);
  assert.ok(Object.isFrozen(rb.stats()));
});
test("malformed configuration preserves complete prior state", () => {
  const rb = new ChangesetRebaser(); rb.configure(rbInfo());
  const before = rb.rebase(local()), stats = rb.stats();
  for (let cut = 1; cut < rbInfo().length; cut++) {
    assert.throws(() => rb.configure(rbInfo().subarray(0, cut)));
    assert.deepEqual(rb.stats(), stats); assert.deepEqual(rb.rebase(local()), before);
  }
});
test("schema mismatch rejects configure and rebase without poisoning history", () => {
  const rb = new ChangesetRebaser(); rb.configure(rbInfo());
  const before = rb.rebase(local()), stats = rb.stats();
  assert.throws(() => rb.configure(encodeRebaseInfo([rbTable(1n, "T", [2, 0, 0])])), { code: "ERR_FSQLITE_REBASE_SCHEMA" });
  assert.throws(() => rb.rebase(encodeChangeset([{ name: "t", primaryKey: [1, 0], changes: [
    { operation: "insert", indirect: false, new: [1n, "v"] },
  ] }])) , { code: "ERR_FSQLITE_REBASE_SCHEMA" });
  assert.deepEqual(rb.stats(), stats); assert.deepEqual(rb.rebase(local()), before);
});
for (const [budget, maximum] of [["maxChanges", 1], ["maxCells", 6], ["maxBytes", 55]]) {
  test(`retained ${budget} fails atomically and capacity is reusable after clear`, () => {
    const rb = new ChangesetRebaser({ [budget]: maximum }); rb.configure(info());
    const stats = rb.stats(), before = rb.rebase(local());
    assert.throws(() => rb.configure(info(2n)), { code: "ERR_FSQLITE_CHANGESET_LIMIT" });
    assert.deepEqual(rb.stats(), stats); assert.deepEqual(rb.rebase(local()), before);
    rb.clear(); rb.configure(info(2n)); assert.equal(rb.stats().changes, 1);
  });
}
test("table budget includes all retained layouts", () => {
  const rb = new ChangesetRebaser({ maxTables: 1 }); rb.configure(info());
  const stats = rb.stats();
  assert.throws(() => rb.configure(encodeRebaseInfo([rbTable(1n, "other")])), { code: "ERR_FSQLITE_CHANGESET_LIMIT" });
  assert.deepEqual(rb.stats(), stats);
});
test("input view offsets are honored and shared/resizable/detached inputs reject", () => {
  const wire = rbInfo(), backing = new Uint8Array(wire.length + 64);
  backing.set(wire, 32); const rb = new ChangesetRebaser();
  rb.configure(backing.subarray(32, 32 + wire.length));
  assert.deepEqual(records(rb.rebase(local())), first.expectedRecords);
  const shared = new Uint8Array(new SharedArrayBuffer(wire.length)); shared.set(wire);
  const resize = new Uint8Array(new ArrayBuffer(wire.length, { maxByteLength: wire.length * 2 })); resize.set(wire);
  const detached = rbInfo(); structuredClone(detached, { transfer: [detached.buffer] });
  for (const input of [shared, resize, detached]) assert.throws(() => rb.configure(input));
});
test("decision indexes and actions are captured once and validated", () => {
  for (const decisions of [null, [{ changeIndex: -1, resolution: "omit" }],
    [{ changeIndex: 0.5, resolution: "omit" }], [{ changeIndex: 1, resolution: "omit" }],
    [{ changeIndex: 0, resolution: "abort" }],
    [{ changeIndex: 0, resolution: "omit" }, { changeIndex: 0, resolution: "replace" }]]) {
    assert.throws(() => createChangesetRebaseInfo(bytes(first.remoteWires[0]), decisions), { code: "ERR_FSQLITE_REBASE_INPUT" });
  }
  let indexReads = 0, actionReads = 0;
  createChangesetRebaseInfo(bytes(first.remoteWires[0]), [{
    get changeIndex() { indexReads++; return 0; },
    get resolution() { actionReads++; return "omit"; },
  }]);
  assert.equal(indexReads, 1); assert.equal(actionReads, 1);
});
test("rebase buffers cannot masquerade as patchsets or contain UPDATE records", () => {
  const rb = new ChangesetRebaser();
  const patch = encodePatchset(decodeChangeset(local()));
  assert.throws(() => rb.configure(patch)); assert.throws(() => rb.rebase(patch));
  const update = encodeChangeset([{ name: "t", primaryKey: [1, 0, 0], changes: [
    { operation: "update", indirect: false, old: [1n, "a", undefined], new: [undefined, "b", undefined] },
  ] }]);
  assert.throws(() => rb.configure(update));
  assert.throws(() => encodeRebaseInfo([{ name: "t", primaryKey: [1, 0, 0], changes: [
    { operation: "update", replace: false, values: [1n, "a", "b"] },
  ] }]));
});
test("missing full-row evidence refuses a synthesized INSERT instead of inventing values", () => {
  const incompleteDelete = encodeRebaseInfo([{ name: "t", primaryKey: [1, 0, 0], changes: [
    { operation: "insert", replace: false, values: [1n, "r", undefined] },
  ] }]);
  incompleteDelete[7] = 9;
  const rb = new ChangesetRebaser(); assert.throws(() => rb.configure(incompleteDelete));
});
for (const [left, right] of [[1n, 1], ["1", 1n], ["A", "a"], [new Uint8Array([0, 1]), new Uint8Array([0, 2])]]) {
  test(`exact typed keys stay independent: ${String(left)} / ${String(right)}`, () => {
    const rb = new ChangesetRebaser(); rb.configure(encodeRebaseInfo([rbTable(left)]));
    const input = encodeChangeset([{ name: "t", primaryKey: [1, 0, 0], changes: [
      { operation: "insert", indirect: false, new: [right, "v", "w"] },
    ] }]);
    assert.deepEqual(rb.rebase(input), input);
  });
}

// Execute shipped SQL, receipts and rebase hooks on Node's actual SQLite.
// This adapter supplies transaction ownership, not an emulated SQL engine.
function sqlTarget(db) {
  let depth = 0, serial = 0;
  const host = {
    loseAcknowledgement: false,
    admissions: 0,
    async transaction(work) {
      const id = `test_rebase_${++serial}`, outer = depth === 0;
      db.exec(outer ? "BEGIN" : `SAVEPOINT ${id}`);
      depth++;
      host.admissions++;
      let active = true, result;
      const tx = {
        async execute(sql, params = []) {
          assert.ok(active, "transaction executor cannot escape its owner");
          return Number(db.prepare(sql).run(...params).changes);
        },
        async query(sql, params = []) {
          assert.ok(active, "transaction executor cannot escape its owner");
          const stmt = db.prepare(sql); stmt.setReadBigInts(true);
          return { rowArrays: stmt.all(...params).map((row) => Object.values(row)) };
        },
      };
      try {
        result = await work(tx);
        db.exec(outer ? "COMMIT" : `RELEASE SAVEPOINT ${id}`);
      } catch (error) {
        try {
          db.exec(outer ? "ROLLBACK" : `ROLLBACK TO SAVEPOINT ${id}; RELEASE SAVEPOINT ${id}`);
        } catch (cleanup) { throw new AggregateError([error, cleanup], "SQLite test transaction cleanup failed"); }
        throw error;
      } finally { active = false; depth--; }
      if (host.loseAcknowledgement) {
        host.loseAcknowledgement = false;
        throw new Error("injected lost commit acknowledgement");
      }
      return result;
    },
  };
  return host;
}
function opened(c = first, path = ":memory:") {
  const db = new DatabaseSync(path);
  db.exec("PRAGMA foreign_keys=ON;" + c.schema + (c.seed ?? "") + c.local);
  db.exec("CREATE TABLE rebase_journal(id TEXT PRIMARY KEY, info BLOB NOT NULL);");
  return { db, host: sqlTarget(db) };
}
const stored = (db, id) => db.prepare("SELECT info FROM rebase_journal WHERE id=?").get(id)?.info;
const rowCount = (db, table) => Number(db.prepare(`SELECT count(*) AS n FROM ${table}`).get().n);
function inboxCount(db) {
  const found = db.prepare("SELECT name FROM sqlite_schema WHERE name='__fsqlite_changeset_receipts'").get();
  return found ? rowCount(db, "__fsqlite_changeset_receipts") : 0;
}

for (const c of generated.cases) {
  test(`transactional native rebase capture: ${c.name}`, async () => {
    const { db, host } = opened(c);
    const rebaser = new ChangesetRebaser();
    try {
      for (let i = 0; i < c.remoteWires.length; i++) {
        const id = `remote:${i}`, wire = bytes(c.remoteWires[i]);
        let hooks = 0;
        const result = await applyChangeset(host, wire, {
          tables: decodeChangeset(wire).map((t) => t.name), deliveryId: id,
          onConflict: () => c.remote[i].policy ?? "omit",
          async onRebase(tx, info) {
            hooks++;
            // No success notification yet: save proof inside the same owner.
            // Node 22's binding of a sliced zero-length buffer may become
            // NULL; explicitly persist a zero-length BLOB for an empty proof.
            if (info.length === 0)
              await tx.execute("INSERT INTO rebase_journal VALUES (?, zeroblob(0))", [id]);
            else await tx.execute("INSERT INTO rebase_journal VALUES (?, ?)", [id, info]);
          },
        });
        assert.equal(result.replayed, false); assert.equal(hooks, 1);
        assert.deepEqual(decodeRebaseInfo(stored(db, id)), decodeRebaseInfo(bytes(c.rebaseBuffers[i])));
        const duplicate = await applyChangeset(host, wire, {
          tables: decodeChangeset(wire).map((t) => t.name), deliveryId: id,
          onConflict: () => assert.fail("replay cannot resolve a conflict again"),
          onRebase: () => assert.fail("replay cannot invent a new decision journal"),
        });
        assert.deepEqual(duplicate, { ...result, replayed: true });
        assert.equal(rowCount(db, "rebase_journal"), i + 1);
        rebaser.configure(stored(db, id));
      }
      assert.deepEqual(records(rebaser.rebase(bytes(c.localWire))), c.expectedRecords);
    } finally { db.close(); }
  });
}

test("journal failure rolls back replacement rows and receipt", async () => {
  const { db, host } = opened();
  try {
    await assert.rejects(applyChangeset(host, bytes(first.remoteWires[0]), {
      tables: ["t"], deliveryId: "failed-journal", onConflict: () => "replace",
      async onRebase(tx, info) {
        await tx.execute("INSERT INTO rebase_journal VALUES (?, ?)", ["failed-journal", info]);
        throw new Error("journal failure");
      },
    }), /journal failure/);
    assert.equal(db.prepare("SELECT v FROM t").get().v, "local");
    assert.equal(rowCount(db, "rebase_journal"), 0); assert.equal(inboxCount(db), 0);
  } finally { db.close(); }
});
test("cancellation after the journal write rolls back every effect", async () => {
  const { db, host } = opened(), controller = new AbortController();
  try {
    await assert.rejects(applyChangeset(host, bytes(first.remoteWires[0]), {
      tables: ["t"], deliveryId: "cancelled", signal: controller.signal, onConflict: () => "replace",
      async onRebase(tx, info) {
        await tx.execute("INSERT INTO rebase_journal VALUES (?, ?)", ["cancelled", info]);
        controller.abort("during journal persistence");
      },
    }), { code: "ERR_FSQLITE_CHANGESET_CANCELLED" });
    assert.equal(db.prepare("SELECT v FROM t").get().v, "local");
    assert.equal(rowCount(db, "rebase_journal"), 0); assert.equal(inboxCount(db), 0);
  } finally { db.close(); }
});
test("deferred COMMIT failure cannot leave a durable journal or receipt", async () => {
  const { db, host } = opened();
  db.exec("CREATE TABLE parent(id PRIMARY KEY); CREATE TABLE deferred_child(id REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED);");
  try {
    await assert.rejects(applyChangeset(host, bytes(first.remoteWires[0]), {
      tables: ["t"], deliveryId: "deferred", onConflict: () => "replace",
      async onRebase(tx, info) {
        await tx.execute("INSERT INTO rebase_journal VALUES (?, ?)", ["deferred", info]);
        await tx.execute("INSERT INTO deferred_child VALUES (?)", [123n]);
      },
    }), /FOREIGN KEY/);
    assert.equal(db.prepare("SELECT v FROM t").get().v, "local");
    assert.equal(rowCount(db, "rebase_journal"), 0); assert.equal(inboxCount(db), 0);
  } finally { db.close(); }
});
test("an outer rollback undoes a successful child application and its journal", async () => {
  const { db, host } = opened();
  try {
    await assert.rejects(host.transaction(async () => {
      await applyChangeset(host, bytes(first.remoteWires[0]), {
        tables: ["t"], deliveryId: "child", onConflict: () => "replace",
        onRebase: (tx, info) => tx.execute("INSERT INTO rebase_journal VALUES (?, ?)", ["child", info]),
      });
      assert.equal(rowCount(db, "rebase_journal"), 1);
      throw new Error("outer failed");
    }), /outer failed/);
    assert.equal(db.prepare("SELECT v FROM t").get().v, "local");
    assert.equal(rowCount(db, "rebase_journal"), 0); assert.equal(inboxCount(db), 0);
  } finally { db.close(); }
});
test("lost acknowledgement plus file reopen recovers the original proof without rerunning SQL", async () => {
  // Retain the temporary database as an inspectable test artifact; no deletion.
  const filename = join(mkdtempSync(join(tmpdir(), "fsqlite-rebase-")), "source.db");
  let { db, host } = opened(first, filename);
  try {
    host.loseAcknowledgement = true;
    await assert.rejects(applyChangeset(host, bytes(first.remoteWires[0]), {
      tables: ["t"], deliveryId: "lost-ack", onConflict: () => "omit",
      onRebase: (tx, info) => tx.execute("INSERT INTO rebase_journal VALUES (?, ?)", ["lost-ack", info]),
    }), /lost commit acknowledgement/);
    db.close(); db = new DatabaseSync(filename); host = sqlTarget(db);
    const replay = await applyChangeset(host, bytes(first.remoteWires[0]), {
      tables: ["t"], deliveryId: "lost-ack",
      onConflict: () => assert.fail("cannot replay policy"),
      onRebase: () => assert.fail("cannot replay persistence"),
    });
    assert.equal(replay.replayed, true);
    assert.equal(rowCount(db, "rebase_journal"), 1); assert.equal(inboxCount(db), 1);
    assert.deepEqual(records(rebaseChangeset(local(), [stored(db, "lost-ack")])), first.expectedRecords);
    assert.equal(db.prepare("PRAGMA integrity_check").get().integrity_check, "ok");
  } finally { db.close(); }
});
test("callback cannot corrupt the reserved inbox schema unnoticed", async () => {
  const { db, host } = opened();
  try {
    await assert.rejects(applyChangeset(host, bytes(first.remoteWires[0]), {
      tables: ["t"], deliveryId: "schema", onConflict: () => "replace",
      async onRebase(tx, info) {
        await tx.execute("INSERT INTO rebase_journal VALUES (?, ?)", ["schema", info]);
        await tx.execute("CREATE TRIGGER bad_receipt AFTER INSERT ON __fsqlite_changeset_receipts BEGIN SELECT 1; END");
      },
    }), { code: "ERR_FSQLITE_CHANGESET_RECEIPT" });
    assert.equal(db.prepare("SELECT v FROM t").get().v, "local");
    assert.equal(rowCount(db, "rebase_journal"), 0); assert.equal(inboxCount(db), 0);
  } finally { db.close(); }
});
test("admission captures limits and hook once before awaiting callbacks", async () => {
  const { db, host } = opened(), input = bytes(first.remoteWires[0]);
  let reads = 0, hooks = 0;
  const options = { tables: ["t"], limits: { get maxBytes() { reads++; return 1000; } },
    async onConflict() { input.fill(0); await Promise.resolve(); return "omit"; },
    onRebase(_tx, info) { hooks++; assert.deepEqual(decodeRebaseInfo(info), decodeRebaseInfo(rbInfo())); },
  };
  try {
    const pending = applyChangeset(host, input, options);
    options.onRebase = () => assert.fail("must not read hook again after admission");
    await pending; assert.equal(reads, 1); assert.equal(hooks, 1);
  } finally { db.close(); }
});
test("patchsets and invalid hooks fail before any transaction admission", async () => {
  const { db, host } = opened();
  try {
    const wire = encodePatchset(decodeChangeset(bytes(first.remoteWires[0])));
    await assert.rejects(applyPatchset(host, wire, { tables: ["t"], onRebase() {} }), { code: "ERR_FSQLITE_CHANGESET_INPUT" });
    await assert.rejects(applyChangeset(host, bytes(first.remoteWires[0]), { tables: ["t"], onRebase: 123 }), { code: "ERR_FSQLITE_CHANGESET_INPUT" });
    assert.equal(host.admissions, 0);
  } finally { db.close(); }
});
test("existing receipts without a decision journal do not fabricate rebase evidence", async () => {
  const { db, host } = opened();
  try {
    await applyChangeset(host, bytes(first.remoteWires[0]), { tables: ["t"], deliveryId: "old", onConflict: () => "omit" });
    const result = await applyChangeset(host, bytes(first.remoteWires[0]), {
      tables: ["t"], deliveryId: "old", onRebase: () => assert.fail("no historical evidence exists"),
    });
    assert.equal(result.replayed, true); assert.equal(rowCount(db, "rebase_journal"), 0);
  } finally { db.close(); }
});

test("key-only duplicate insert needs no impossible empty UPDATE", async () => {
  const db = new DatabaseSync(":memory:"), host = sqlTarget(db);
  db.exec("CREATE TABLE key_only(k PRIMARY KEY); INSERT INTO key_only VALUES(1);");
  const wire = encodeChangeset([{ name: "key_only", primaryKey: [1], changes: [
    { operation: "insert", indirect: false, new: [1n] },
  ] }]);
  let captured;
  try {
    await applyChangeset(host, wire, {
      tables: ["key_only"], onConflict: () => "omit", onRebase(_tx, info) { captured = info; },
    });
    assert.equal(rebaseChangeset(wire, [captured]).length, 0);
    assert.equal(rowCount(db, "key_only"), 1);
  } finally { db.close(); }
});
test("an empty fresh delivery saves an explicit empty decision buffer once", async () => {
  const { db, host } = opened();
  let calls = 0;
  const options = { tables: [], deliveryId: "empty",
    async onRebase(tx, info) {
      calls++; assert.equal(info.length, 0);
      await tx.execute("INSERT INTO rebase_journal VALUES ('empty', zeroblob(0))");
    },
  };
  try {
    assert.deepEqual(await applyChangeset(host, new Uint8Array(), options), { applied: 0, omitted: 0, replayed: false });
    assert.deepEqual(await applyChangeset(host, new Uint8Array(), options), { applied: 0, omitted: 0, replayed: true });
    assert.equal(calls, 1); assert.equal(stored(db, "empty").length, 0);
  } finally { db.close(); }
});
test("shared codec preserves 64 native changeset/patchset roundtrips and default patchset application", async () => {
  const initial = "CREATE TABLE c(k PRIMARY KEY,v,w); INSERT INTO c VALUES(1,'a','b'),(2,'c','d');";
  const rows = (db) => {
    const statement = db.prepare("SELECT k,v,w,typeof(v) FROM c ORDER BY k");
    statement.setReadBigInts(true); return statement.all();
  };
  for (let n = 0; n < 64; n++) {
    const source = new DatabaseSync(":memory:"), replica = new DatabaseSync(":memory:");
    source.exec(initial); replica.exec(initial);
    const session = source.createSession();
    try {
      source.exec(`UPDATE c SET v=CAST(${n} AS REAL),w=x'00ff'; DELETE FROM c WHERE k=2; INSERT INTO c VALUES(3,'λ'||char(0)||'😀',NULL);`);
      const full = session.changeset(), patch = session.patchset();
      assert.deepEqual(encodeChangeset(decodeChangeset(full)), Uint8Array.from(full));
      assert.deepEqual(encodePatchset(decodePatchset(patch)), Uint8Array.from(patch));
      const result = await applyPatchset(sqlTarget(replica), patch, { tables: ["c"] });
      assert.equal(result.applied, 3); assert.deepEqual(rows(replica), rows(source));
    } finally { session.close(); source.close(); replica.close(); }
  }
});
