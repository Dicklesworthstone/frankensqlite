// Production grouping against native SQLite changegroup/session/apply APIs.
// Requires Node's node:sqlite and Python with a session-enabled libsqlite3.
// This executes the SDK algorithms; it does not qualify browser/WASM storage.
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import {
  decodeChangeset, decodePatchset, encodeChangeset, encodePatchset, invertChangeset,
} from "../src/changeset-codec.ts";
import {
  ChangesetGroup, ChangesetGroupError, PatchsetGroup, concatChangesets, concatPatchsets,
} from "../src/changeset-group.ts";

// No emulation of changegroup semantics: each prefix is output by the actual
// C library. Calls use explicit lengths and keep input buffers alive throughout.
const oracle = String.raw`
import ctypes as C, ctypes.util, json, sys
lib = C.CDLL(ctypes.util.find_library('sqlite3'))
P, I = C.c_void_p, C.c_int
for name, result, args in [
    ('sqlite3_libversion', C.c_char_p, []),
    ('sqlite3changegroup_new', I, [C.POINTER(P)]),
    ('sqlite3changegroup_add', I, [P, I, P]),
    ('sqlite3changegroup_output', I, [P, C.POINTER(I), C.POINTER(P)]),
    ('sqlite3changegroup_delete', None, [P]),
    ('sqlite3_free', None, [P]),
]:
    f = getattr(lib, name)
    f.restype, f.argtypes = result, args

def check(rc):
    if rc != 0:
        raise RuntimeError('native changegroup returned SQLite code ' + str(rc))

def group(parts):
    owner = P()
    check(lib.sqlite3changegroup_new(C.byref(owner)))
    results = []
    try:
        for part in parts:
            wire = bytes.fromhex(part)
            buffer = C.create_string_buffer(wire)
            check(lib.sqlite3changegroup_add(owner, len(wire), buffer))
            size, pointer = I(), P()
            check(lib.sqlite3changegroup_output(owner, C.byref(size), C.byref(pointer)))
            try:
                results.append(C.string_at(pointer, size.value).hex() if size.value else '')
            finally:
                lib.sqlite3_free(pointer)
        return results
    finally:
        lib.sqlite3changegroup_delete(owner)

print(json.dumps({'sqlite': lib.sqlite3_libversion().decode(),
                  'groups': [group(parts) for parts in json.load(sys.stdin)]}))
`;
const hex = bytes => Buffer.from(bytes).toString("hex");
const unhex = text => new Uint8Array(Buffer.from(text, "hex"));
const empty = () => new Uint8Array();
const insert = (values, indirect = false) => ({ operation: "insert", indirect, new: values });
const remove = (values, indirect = false) => ({ operation: "delete", indirect, old: values });
const update = (old, next, indirect = false) => ({ operation: "update", indirect, old, new: next });
const table = (changes, primaryKey = [1, 0], name = "t") => ({ name, primaryKey, changes });
const wire = (changes, pk = [1, 0], name = "t") => encodePatchset([table(changes, pk, name)]);
const limitError = { code: "ERR_FSQLITE_CHANGESET_LIMIT" };

function nativeGroups(groups) {
  const result = spawnSync(process.env.PYTHON ?? "python3", ["-c", oracle], {
    input: JSON.stringify(groups.map(parts => parts.map(hex))),
    encoding: "utf8", timeout: 30_000, maxBuffer: 32 * 1024 * 1024,
  });
  assert.ifError(result.error);
  assert.equal(result.status, 0, result.stderr);
  return JSON.parse(result.stdout);
}

// Ignore ONLY hash iteration order, not field types, absent values, flags or
// key-position bytes. Round-trip every row through the same format's encoder.
function normalized(bytes, patchset = true) {
  const decode = patchset ? decodePatchset : decodeChangeset;
  const encode = patchset ? encodePatchset : encodeChangeset;
  return decode(bytes).map(t => ({
    name: t.name, primaryKey: t.primaryKey,
    rows: t.changes.map(c => hex(encode([{ ...t, changes: [c] }]))).sort(),
  })).sort((a, b) => a.name.localeCompare(b.name));
}

function compareGroups(groups, t, patchset = true) {
  const reference = nativeGroups(groups);
  const Group = patchset ? PatchsetGroup : ChangesetGroup;
  const concat = patchset ? concatPatchsets : concatChangesets;
  const decode = patchset ? decodePatchset : decodeChangeset;
  let prefixes = 0;
  groups.forEach((parts, scenario) => {
    const group = new Group();
    let composed = empty();
    parts.forEach((part, index) => {
      const stats = group.add(part);
      composed = concat(composed, part);
      const result = group.output();
      const expected = normalized(unhex(reference.groups[scenario][index]), patchset);
      assert.deepEqual(normalized(result, patchset), expected, `group=${scenario} prefix=${index}`);
      assert.deepEqual(normalized(composed, patchset), expected, `concat=${scenario} prefix=${index}`);
      assert.equal(stats.byteLength, result.length);
      assert.equal(stats.changes, decode(result).reduce((n, x) => n + x.changes.length, 0));
      prefixes++;
    });
  });
  t.diagnostic(`SQLite changegroup ${reference.sqlite}: ${groups.length} histories, ${prefixes} prefixes; both grouping APIs compared`);
}

test("all nine patchset transitions and indirect flags match native changegroup", t => {
  const groups = [];
  const keys = [-(1n << 63n), (1n << 63n) - 1n, 1, "λ\0🚀", new Uint8Array([0, 255]), 0, -0, ""];
  for (const key of keys) for (const ai of [false, true]) for (const bi of [false, true]) {
    const a = [insert([key, "initial"], ai), update([key, undefined], [undefined, null], ai), remove([key, undefined], ai)];
    const b = [insert([key, new Uint8Array([2])], bi), update([key, undefined], [undefined, 7.5], bi), remove([key, undefined], bi)];
    for (const first of a) for (const second of b) groups.push([wire([first]), wire([second])]);
  }
  compareGroups(groups, t);
});

test("three-step compact histories with disjoint columns match native changegroup", t => {
  const groups = [];
  const pk = [0, 2, 1, 0];
  const old = [undefined, new Uint8Array([0, 255]), "composite\0key", undefined];
  const change = (kind, step, indirect) => {
    if (kind === 0) return insert([null, old[1], old[2], BigInt(step)], indirect);
    if (kind === 1) return remove(old, indirect);
    return update(old,
      kind === 2 ? [new Uint8Array([step]), undefined, undefined, undefined]
        : [undefined, undefined, undefined, step === 1 ? 7.5 : null], indirect);
  };
  for (let a = 0; a < 4; a++) for (let b = 0; b < 4; b++) for (let c = 0; c < 4; c++) {
    for (let flags = 0; flags < 8; flags++) groups.push([a, b, c].map((kind, step) =>
      wire([change(kind, step, Boolean(flags & (1 << step)))], pk)));
  }
  compareGroups(groups, t);
});

test("native captured patchsets replay to exact SQL rows and full changesets still invert", t => {
  let state = 0x72f516ab;
  const random = () => { state ^= state << 13; state ^= state >>> 17; state ^= state << 5; return state >>> 0; };
  const values = [null, "", "λ\0🚀", new Uint8Array(), new Uint8Array([0, 255]), 0n, 1n, -1n, (1n << 63n) - 1n, 1.5, -Infinity];
  const schema = "CREATE TABLE t(id INTEGER PRIMARY KEY, a, b);";
  const seed = "INSERT INTO t VALUES(1,'initial',7),(2,NULL,X'00');";
  const snapshot = db => {
    const statement = db.prepare("SELECT * FROM t ORDER BY id");
    statement.setReadBigInts(true);
    return statement.all();
  };
  const patchHistories = [], fullHistories = [];
  for (let run = 0; run < 16; run++) {
    const source = new DatabaseSync(":memory:");
    source.exec(schema + seed);
    const initial = snapshot(source);
    const whole = source.createSession();
    const patchParts = [], fullParts = [];
    const patches = new PatchsetGroup(), changes = new ChangesetGroup();
    try {
      for (let step = 0; step < 12; step++) {
        const session = source.createSession();
        try {
          for (let n = 0; n < 8; n++) {
            const id = 1 + random() % 16, value = values[random() % values.length];
            switch (random() % 4) {
              case 0: source.prepare("INSERT OR IGNORE INTO t VALUES(?,?,?)").run(id, value, value); break;
              case 1: source.prepare("UPDATE t SET a=? WHERE id=?").run(value, id); break;
              case 2: source.prepare("UPDATE t SET b=? WHERE id=?").run(value, id); break;
              default: source.prepare("DELETE FROM t WHERE id=?").run(id); break;
            }
          }
          patchParts.push(new Uint8Array(session.patchset()));
          fullParts.push(new Uint8Array(session.changeset()));
        } finally { session.close(); }
        patches.add(patchParts.at(-1));
        changes.add(fullParts.at(-1));
        assert.deepEqual(normalized(changes.output(), false), normalized(whole.changeset(), false));
        for (const compact of [true, false]) {
          const replica = new DatabaseSync(":memory:");
          try {
            replica.exec(schema + seed);
            const output = compact ? patches.output() : changes.output();
            assert.equal(replica.applyChangeset(output), true);
            assert.deepEqual(snapshot(replica), snapshot(source));
            if (!compact) {
              assert.equal(replica.applyChangeset(invertChangeset(output)), true);
              assert.deepEqual(snapshot(replica), initial);
            }
          } finally { replica.close(); }
        }
      }
      patchHistories.push(patchParts);
      fullHistories.push(fullParts);
    } finally { whole.close(); source.close(); }
  }
  compareGroups(patchHistories, t);
  compareGroups(fullHistories, t, false);
  const version = new DatabaseSync(":memory:");
  try { t.diagnostic(`SQLite session/apply ${version.prepare('SELECT sqlite_version() v').get().v}: 1536 SQL mutation statements, 384 applied prefix images, 192 inversions`); }
  finally { version.close(); }
});

test("unknown before-images never become NULL and apparent reversions remain updates", () => {
  const group = new PatchsetGroup();
  group.add(wire([update([1n, undefined, undefined], [undefined, "middle", undefined])], [1, 0, 0]));
  group.add(wire([update([1n, undefined, undefined], [undefined, "original", null])], [1, 0, 0]));
  assert.deepEqual(decodePatchset(group.output())[0].changes, [
    update([1n, undefined, undefined], [undefined, "original", null]),
  ]);
  group.add(wire([remove([1n, undefined, undefined])], [1, 0, 0]));
  assert.deepEqual(decodePatchset(group.output())[0].changes, [remove([1n, undefined, undefined])]);
  group.add(wire([insert([1n, null, "original"])], [1, 0, 0]));
  assert.deepEqual(decodePatchset(group.output())[0].changes, [
    update([1n, undefined, undefined], [undefined, null, "original"]),
  ]);
});

test("compact wire accounting and normalized cell budgets agree at varint boundaries", () => {
  for (const columns of [2, 127, 128, 2000]) {
    const pk = Array(columns).fill(0); pk[columns - 1] = 1;
    const keys = Array(columns).fill(undefined); keys[columns - 1] = "key\0λ";
    for (const size of [0, 127, 128, 16384]) {
      const before = Array(columns).fill(null); before[columns - 1] = keys[columns - 1];
      const next = Array(columns).fill(undefined); next[0] = new Uint8Array(size).fill(7);
      for (const change of [insert(before), update(keys, next), remove(keys)]) {
        const part = wire([change], pk, "表λ🚀");
        const cells = columns * (change.operation === "update" ? 2 : 1);
        const group = new PatchsetGroup({ maxBytes: part.length, maxCells: cells });
        assert.equal(group.add(part).byteLength, part.length);
        assert.equal(group.stats().cells, cells);
        assert.deepEqual(group.output(), part);
        assert.throws(() => new PatchsetGroup({ maxBytes: part.length - 1 }).add(part), limitError);
        assert.throws(() => new PatchsetGroup({ maxCells: cells - 1 }).add(part), limitError);
      }
    }
  }
});

test("late budget and schema failures leave staged changes and counters untouched", () => {
  const deletion = wire([remove([1n, undefined])]);
  const insertion = wire([insert([1n, "new"])]);
  const group = new PatchsetGroup({ maxCells: 3 });
  group.add(deletion);
  const stats = group.stats();
  // Both inputs use two normalized slots; DELETE->INSERT becomes a four-slot UPDATE.
  assert.throws(() => group.add(insertion), limitError);
  assert.deepEqual(group.output(), deletion);
  assert.equal(group.stats(), stats);

  const bounded = new PatchsetGroup({ maxBytes: 90 });
  bounded.add(wire([1n, 2n, 3n].map(id => insert([id, "a".repeat(10)]))));
  const before = bounded.output(), count = bounded.stats();
  const chunk = wire([remove([1n, undefined]), insert([4n, "b".repeat(26)])]);
  assert.ok(chunk.length <= 90);
  assert.throws(() => bounded.add(chunk), limitError);
  assert.deepEqual(bounded.output(), before);
  assert.equal(bounded.stats(), count);

  const schemas = new PatchsetGroup();
  schemas.add(encodePatchset([table([insert([1n, "a"])]), table([insert([1n, "b"])], [1, 0], "u")]));
  const original = schemas.output(), oldStats = schemas.stats();
  assert.throws(() => schemas.add(encodePatchset([
    table([remove([1n, undefined])]), table([insert([2n, "invalid", null])], [1, 0, 0], "u"),
  ])), ChangesetGroupError);
  assert.deepEqual(schemas.output(), original);
  assert.equal(schemas.stats(), oldStats);
  schemas.add(wire([update([1n, undefined], [undefined, "valid"])]));
  assert.equal(decodePatchset(schemas.output())[0].changes[0].new[1], "valid");
});

test("retained schema identity, case folding, cancellation and clear share one bounded core", () => {
  const group = new PatchsetGroup({ maxTables: 1, maxChanges: 2 });
  group.add(wire([insert([1n, "a"]), insert([2n, "b"])], [1, 0], "TaBlE"));
  group.add(wire([insert([3n, "c"]), remove([1n, undefined])], [1, 0], "table"));
  assert.equal(decodePatchset(group.output())[0].name, "TaBlE");
  group.add(wire([remove([2n, undefined]), remove([3n, undefined])], [1, 0], "table"));
  assert.equal(group.stats().changes, 0);
  assert.equal(group.stats().tables, 1);
  assert.equal(group.stats().byteLength, 0);
  assert.ok(group.stats().schemaBytes > 0);
  assert.throws(() => group.add(wire([insert([1n, null])], [1, 0], "other")), limitError);
  assert.throws(() => group.add(wire([insert([1n, null])], [2, 0], "TABLE")), ChangesetGroupError);
  group.clear();
  assert.deepEqual(group.stats(), { tables: 0, changes: 0, cells: 0, byteLength: 0, schemaBytes: 0 });
  group.add(wire([insert([1n, null])], [1, 0], "other"));
});

test("cancelled rows cannot evade retained schema-byte limits", () => {
  const group = new PatchsetGroup({ maxBytes: 34, maxTables: 100 });
  const cancelled = name => wire([insert([1n, "a"]), remove([1n, undefined])], [1, 0], name);
  for (const name of ["t", "u", "v", "w", "x"]) group.add(cancelled(name));
  assert.equal(group.stats().schemaBytes, 30);
  assert.equal(group.output().length, 0);
  const stats = group.stats();
  assert.throws(() => group.add(cancelled("y")), limitError);
  assert.equal(group.stats(), stats);
});

test("formats never mix and rejected buffers cannot mutate existing state", () => {
  const patch = wire([insert([1n, "payload"])]);
  const full = encodeChangeset([table([insert([2n, "other"])])]);
  const group = new PatchsetGroup(); group.add(patch);
  const stats = group.stats(), before = group.output();
  const detached = patch.slice(); structuredClone(detached.buffer, { transfer: [detached.buffer] });
  for (const bad of [full, patch.slice(0, -1), detached, new Proxy(patch, {}), new Uint8Array(new SharedArrayBuffer(0))]) {
    assert.throws(() => group.add(bad));
    assert.deepEqual(group.output(), before); assert.equal(group.stats(), stats);
  }
  assert.throws(() => new ChangesetGroup().add(patch));
  assert.throws(() => concatChangesets(patch, empty()));
  assert.throws(() => concatPatchsets(empty(), full));
  assert.throws(() => invertChangeset(patch));
  group.clear(); assert.throws(() => group.add(full));
});

test("inputs, outputs, getter capture and hostile typed arrays preserve ownership", () => {
  const reads = new Map(), options = {};
  for (const name of ["maxBytes", "maxTables", "maxColumns", "maxChanges", "maxCells"]) {
    Object.defineProperty(options, name, { get() { reads.set(name, (reads.get(name) ?? 0) + 1); return undefined; } });
  }
  const group = new PatchsetGroup(options);
  const part = wire([insert([new Uint8Array([0, 255]), new Uint8Array([7, 8])])]);
  let calls = 0;
  for (const name of ["length", "byteLength", "byteOffset", "buffer", "subarray"]) {
    Object.defineProperty(part, name, { get() { calls++; group.clear(); throw new Error("must not call"); } });
  }
  group.add(part);
  assert.equal(calls, 0);
  const output = group.output(), expected = output.slice(), stats = group.stats();
  part.fill(0); output.fill(0);
  assert.deepEqual(group.output(), expected);
  assert.ok(Object.isFrozen(stats));
  assert.throws(() => { stats.changes = 0; }, TypeError);
  group.clear(); group.add(expected); group.output();
  assert.ok([...reads.values()].every(n => n === 1));
  reads.clear();
  assert.deepEqual(concatPatchsets(expected, empty(), options), expected);
  assert.ok([...reads.values()].every(n => n === 1));
});

test("typed and length-delimited composite keys cannot merge unrelated changes", t => {
  const keys = [1n, 1, "1", new Uint8Array([49]), 0, -0, "", new Uint8Array()];
  const a = wire(keys.map(key => insert([key, "old"])));
  const b = wire(keys.map(key => update([key, undefined], [undefined, "new"])));
  compareGroups([[a, b]], t);
  assert.equal(decodePatchset(concatPatchsets(a, b))[0].changes.length, keys.length);
  const composite = [["a", "b:c"], ["a:b", "c"], ["", "t0:"], ["t0:", ""], [new Uint8Array([1, 2]), "x"]];
  compareGroups([[
    wire(composite.map(key => insert([...key, "before"])), [1, 2, 0]),
    wire(composite.map(key => update([...key, undefined], [undefined, undefined, "after"])), [1, 2, 0]),
  ]], t);
});

test("all-key DELETE/INSERT cannot poison a valid incremental group with an unusable UPDATE", t => {
  const removed = wire([remove([1n])], [1]);
  const added = wire([insert([1n])], [1]);
  const reference = nativeGroups([[removed, added]]);
  const nativeOutput = unhex(reference.groups[0][1]);
  // Pin this limitation to actual library behavior, not an invented fixture.
  assert.throws(() => decodePatchset(nativeOutput), { code: "ERR_FSQLITE_CHANGESET_FORMAT" });
  const db = new DatabaseSync(":memory:");
  try {
    db.exec("CREATE TABLE t(k PRIMARY KEY); INSERT INTO t VALUES(1)");
    assert.throws(() => db.applyChangeset(nativeOutput));
  } finally { db.close(); }
  const group = new PatchsetGroup(); group.add(removed);
  const stats = group.stats();
  assert.throws(() => group.add(added), { code: "ERR_FSQLITE_CHANGESET_FORMAT" });
  assert.deepEqual(group.output(), removed); assert.equal(group.stats(), stats);
  assert.throws(() => concatPatchsets(removed, added), { code: "ERR_FSQLITE_CHANGESET_FORMAT" });
  group.clear(); group.add(added); group.add(removed); assert.deepEqual(group.output(), empty());
  t.diagnostic(`Native ${reference.sqlite} all-key output is deliberately refused; codec validation remains unchanged`);
});
