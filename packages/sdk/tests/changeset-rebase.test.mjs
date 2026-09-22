import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { test } from "node:test";
import { decodeChangeset, decodeRebaseInfo, encodeChangeset, encodePatchset, encodeRebaseInfo } from "../src/changeset-codec.ts";
import { ChangesetRebaser, createChangesetRebaseInfo, rebaseChangeset } from "../src/changeset-rebase.ts";

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
