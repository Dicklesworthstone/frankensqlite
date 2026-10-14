// Execute the production codec; native SQLite applies full and compact output.
// This is SDK wire/input qualification, not browser/WASM or Rust-engine testing.
import assert from "node:assert/strict";
import { Buffer } from "node:buffer";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import {
  decodeChangeset, decodePatchset, decodeRebaseInfo,
  encodeChangeset, encodePatchset, encodeRebaseInfo,
} from "../src/changeset-codec.ts";

const formats = [
  { name: "changeset", encode: encodeChangeset, decode: decodeChangeset },
  { name: "patchset", encode: encodePatchset, decode: decodePatchset },
  { name: "rebase", encode: encodeRebaseInfo, decode: decodeRebaseInfo },
];
function record(format, key, blob) {
  const change = format.name === "rebase"
    ? { operation: "insert", replace: false, values: [key, blob, "after-blob"] }
    : { operation: "insert", indirect: false, new: [key, blob, "after-blob"] };
  return [{ name: "t", primaryKey: [1, 0, 0], changes: [change] }];
}
function values(format, bytes) {
  const change = format.decode(bytes)[0].changes[0];
  return format.name === "rebase" ? change.values : change.new;
}
function encoded(format, blob, options) {
  return format.encode(record(format, 1n, blob), options);
}
const inputError = { code: "ERR_FSQLITE_CHANGESET_INPUT" };
const limitError = { code: "ERR_FSQLITE_CHANGESET_LIMIT" };

test("shadowed BLOB length cannot silently replace replicated data with an empty BLOB", t => {
  for (const format of formats) {
    const blob = new Uint8Array([0, 255, 7, 8]);
    Object.defineProperty(blob, "length", { value: 0 });
    const bytes = encoded(format, blob);
    assert.deepEqual(values(format, bytes), [1n, new Uint8Array([0, 255, 7, 8]), "after-blob"], format.name);
    if (format.name === "rebase") continue;
    const db = new DatabaseSync(":memory:");
    try {
      db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY,v,tail)");
      assert.equal(db.applyChangeset(bytes), true);
      const row = db.prepare("SELECT hex(v) v, typeof(v) kind, tail FROM t").get();
      assert.deepEqual({ ...row }, { v: "00FF0708", kind: "blob", tail: "after-blob" });
      t.diagnostic(`${format.name}: native SQLite ${db.prepare("SELECT sqlite_version() v").get().v} stores the exact BLOB`);
    } finally { db.close(); }
  }
});

test("all session encoders use internal BLOB slots without calling shadowed accessors", () => {
  for (const format of formats) {
    const storage = new Uint8Array([90, 90, 0, 255, 7, 90]);
    const blob = storage.subarray(2, 5);
    let calls = 0;
    for (const name of ["length", "byteLength", "byteOffset", "buffer", "subarray", "slice", "set"]) {
      Object.defineProperty(blob, name, { get() { calls++; throw new Error(`unexpected ${name} getter`); } });
    }
    Object.defineProperty(blob, Symbol.iterator, { get() { calls++; throw new Error("unexpected iterator"); } });
    const bytes = encoded(format, blob);
    assert.equal(calls, 0, format.name);
    assert.deepEqual(values(format, bytes)[1], new Uint8Array([0, 255, 7]));
    assert.deepEqual(storage, new Uint8Array([90, 90, 0, 255, 7, 90]));
    storage.fill(0);
    assert.deepEqual(values(format, bytes)[1], new Uint8Array([0, 255, 7]), "output owns its bytes");
  }
});

test("Uint8Array subclasses and Node Buffer slices preserve exact payload and composite keys", () => {
  class Input extends Uint8Array {
    get length() { throw new Error("subclass length is not storage authority"); }
    get byteOffset() { throw new Error("subclass offset is not storage authority"); }
    get buffer() { throw new Error("subclass buffer is not storage authority"); }
  }
  for (const format of formats) {
    const blob = new Input([3, 4, 5]);
    assert.deepEqual(values(format, encoded(format, blob))[1], new Uint8Array([3, 4, 5]));
    const backing = Buffer.from([9, 1, 2, 9]);
    const key = backing.subarray(1, 3);
    Object.defineProperty(key, "length", { value: 0 });
    const bytes = format.encode(record(format, key, blob));
    assert.deepEqual(values(format, bytes), [new Uint8Array([1, 2]), new Uint8Array([3, 4, 5]), "after-blob"]);
    assert.deepEqual([...backing], [9, 1, 2, 9]);
  }
});

test("shared, resizable, detached and proxied BLOBs are refused in all session formats", () => {
  for (const size of [0, 3]) for (const format of formats) {
    const detached = new Uint8Array(size);
    structuredClone(detached.buffer, { transfer: [detached.buffer] });
    const shared = new Uint8Array(new SharedArrayBuffer(size));
    Object.defineProperty(shared, "buffer", { value: new ArrayBuffer(size) });
    const resizable = new ArrayBuffer(size, { maxByteLength: size + 10 });
    Object.defineProperty(resizable, "resizable", { value: false });
    for (const blob of [detached, shared, new Uint8Array(resizable), new Proxy(new Uint8Array(size), {})]) {
      assert.throws(() => encoded(format, blob), inputError, `${format.name}/${size}`);
    }
    // Fixed zero-length BLOBs remain valid; no blanket empty-value rejection.
    assert.deepEqual(values(format, encoded(format, new Uint8Array()))[1], new Uint8Array());
  }
});

test("real BLOB size controls exact output budgets despite forged public metadata", () => {
  for (const format of formats) for (const size of [1, 127, 128, 16383, 16384]) {
    const original = new Uint8Array(size).fill(13);
    const expected = encoded(format, original);
    const blob = new Uint8Array(original);
    for (const name of ["length", "byteLength", "byteOffset"]) Object.defineProperty(blob, name, { value: 0 });
    Object.defineProperty(blob, "buffer", { value: new ArrayBuffer(0) });
    assert.deepEqual(encoded(format, blob, { maxBytes: expected.length }), expected);
    assert.throws(() => encoded(format, blob, { maxBytes: expected.length - 1 }), limitError);
    assert.deepEqual(original, new Uint8Array(size).fill(13));
  }
});

test("oversized forged BLOBs fail with a typed budget error and cannot overwrite later fields", () => {
  for (const format of formats) {
    const blob = new Uint8Array(2048).fill(18);
    Object.defineProperty(blob, "length", { value: 0 });
    assert.throws(() => encoded(format, blob, { maxBytes: 64 }), limitError);
    const tables = record(format, 1n, new Uint8Array([1]));
    tables[0].changes.push(record(format, 2n, blob)[0].changes[0]);
    assert.throws(() => format.encode(tables, { maxBytes: 64 }), limitError);
    assert.deepEqual(values(format, encoded(format, new Uint8Array([2])))[1], new Uint8Array([2]));
  }
});

test("normal and shadowed sliced BLOB wire is byte-identical to native Session output", () => {
  for (const size of [0, 1, 127, 128, 16383, 16384]) {
    const source = new DatabaseSync(":memory:");
    try {
      source.exec("CREATE TABLE t(id INTEGER PRIMARY KEY,v,tail)");
      const session = source.createSession();
      try {
        const backing = new Uint8Array(size + 4).fill(9);
        const blob = backing.subarray(2, 2 + size);
        blob.fill(23);
        source.prepare("INSERT INTO t VALUES(?,?,?)").run(1n, blob, "after-blob");
        Object.defineProperty(blob, "length", { value: 0 });
        assert.deepEqual(encoded(formats[0], blob), new Uint8Array(session.changeset()));
        assert.deepEqual(encoded(formats[1], blob), new Uint8Array(session.patchset()));
      } finally { session.close(); }
    } finally { source.close(); }
  }
});
