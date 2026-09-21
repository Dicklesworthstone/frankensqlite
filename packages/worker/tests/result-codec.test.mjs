import assert from "node:assert/strict";
import { test } from "node:test";
import {
  BINARY_RESULT_THRESHOLD,
  decodeQueryResult,
  MAX_BINARY_RESULT_BYTES,
  ResultCodecError,
  resolveResultEncoding,
  tryEncodeQueryResult,
} from "../src/result-codec.ts";

export function result(columns, rowArrays, columnTypes = [], changes = 0) {
  const rows = rowArrays.map((values) =>
    Object.fromEntries(columns.map((name, j) => [name, values[j]])),
  );
  return { columns, columnCount: columns.length, columnTypes, rowArrays, rows, changes };
}
function roundtrip(value) {
  const encoded = tryEncodeQueryResult(value);
  assert.ok(encoded instanceof ArrayBuffer);
  assert.deepEqual(decodeQueryResult(encoded), value);
  return encoded;
}

test("binary scalars retain distinct numbers, BigInts, booleans and null", () => {
  const values = [
    null,
    false,
    true,
    0,
    -0,
    NaN,
    Infinity,
    -Infinity,
    Number.MIN_VALUE,
    Number.MAX_VALUE,
    17.5,
    Number.MAX_SAFE_INTEGER,
    0n,
    -(1n << 63n),
    (1n << 63n) - 1n,
  ];
  const encoded = roundtrip(
    result(
      ["v"],
      values.map((v) => [v]),
      ["mixed"],
      123,
    ),
  );
  const decoded = decodeQueryResult(encoded);
  assert.ok(Object.is(decoded.rowArrays[4][0], -0));
  assert.equal(typeof decoded.rowArrays[12][0], "bigint");
});

test("binary strings preserve UTF-16, NUL, BOM, lone surrogates and long values", () => {
  roundtrip(
    result(
      ["\ud800\ufeff"],
      [
        "",
        "plain",
        "世界",
        "🦀🧬",
        "\0x\0",
        "\ufeff",
        "\ud800",
        "\udfff",
        "a\ud800b\udfff",
        "abc".repeat(20_000),
      ].map((v) => [v]),
      ["text\0\ud800"],
    ),
  );
});

test("empty results and zero-column rows roundtrip without inventing values", () => {
  roundtrip(result(["a", "b"], [], ["integer", "text"]));
  roundtrip(result([], [[], [], []]));
  roundtrip(result([], []));
});

test("duplicate columns retain both positional values and last-column-wins object rows", () => {
  roundtrip(
    result(
      ["a", "b", "a"],
      [
        [1, "x", 2],
        [3, "y", null],
      ],
    ),
  );
});

test("prototype-like column names stay own data properties and never alter object prototypes", () => {
  const value = result(
    ["__proto__", "constructor", "", "toString", "0"],
    [["text", "c", "empty", null, 0]],
  );
  const decoded = decodeQueryResult(roundtrip(value));
  assert.equal(Object.getPrototypeOf(decoded.rows[0]), Object.prototype);
  assert.equal(Object.hasOwn(decoded.rows[0], "__proto__"), true);
  assert.equal(decoded.rows[0].__proto__, "text");
});

test("transfer detaches only the new packet, not source buffers or their larger backing allocation", () => {
  const source = new Uint8Array(1024 * 1024);
  source.set([4, 5, 6], 17);
  const value = result(["b"], [[source.subarray(17, 20)], [new Uint8Array(0)]]);
  const frame = roundtrip(value);
  assert.ok(frame.byteLength < 100);
  const received = structuredClone(frame, { transfer: [frame] });
  assert.equal(frame.byteLength, 0);
  assert.equal(source.byteLength, 1024 * 1024);
  const decoded = decodeQueryResult(received);
  assert.deepEqual([...decoded.rowArrays[0][0]], [4, 5, 6]);
  assert.equal(decoded.rowArrays[0][0].buffer.byteLength, 3);
  structuredClone(received, { transfer: [received] });
  assert.deepEqual([...decoded.rowArrays[0][0]], [4, 5, 6]);
  assert.equal(decoded.rows[0].b, decoded.rowArrays[0][0]);
});

test("projection checks compare equal distinct blob objects without discarding either value", () => {
  const value = result(["b"], [[Uint8Array.of(1, 2)]]);
  value.rows[0].b = Uint8Array.of(1, 2);
  roundtrip(value);
  value.rows[0].b[0] = 99;
  assert.equal(tryEncodeQueryResult(value), null);
});

test("auto threshold uses complete encoded cost and binary size is bounded", () => {
  const small = result(["v"], [[1]]);
  const size = roundtrip(small).byteLength;
  assert.equal(tryEncodeQueryResult(small, size + 1), null);
  assert.ok(tryEncodeQueryResult(small, size));
  assert.equal(tryEncodeQueryResult(small, BINARY_RESULT_THRESHOLD), null);
  assert.ok(
    tryEncodeQueryResult(
      result(["v"], [["x".repeat(BINARY_RESULT_THRESHOLD)]]),
      BINARY_RESULT_THRESHOLD,
    ),
  );
  assert.equal(
    tryEncodeQueryResult(result(["b"], [[new Uint8Array(MAX_BINARY_RESULT_BYTES)]])),
    null,
  );
});

test("unsupported values and additional metadata take lossless structured-clone fallback", () => {
  for (const value of [undefined, 1n << 64n, -(1n << 63n) - 1n, { nested: true }, new Date(0)]) {
    assert.equal(tryEncodeQueryResult(result(["v"], [[value]])), null);
  }
  const value = result(["v"], [[1]]);
  assert.equal(tryEncodeQueryResult({ ...value, extension: { revision: 7 } }), null);
  value.rows[0].extra = 1;
  assert.equal(tryEncodeQueryResult(value), null);
});

test("noncanonical projections, missing cells and inconsistent metadata fall back", () => {
  for (const edit of [
    (r) => {
      r.rows[0].v = 9;
    },
    (r) => {
      delete r.rows[0].v;
    },
    (r) => {
      r.rowArrays[0] = [];
    },
    (r) => {
      r.columnCount = 2;
    },
    (r) => {
      r.rows = [];
    },
    (r) => {
      r.columns = [null];
    },
    (r) => {
      r.columnTypes = [null];
    },
    (r) => {
      r.changes = -1;
    },
    (r) => {
      r.rowArrays[0] = new Array(1);
    },
  ]) {
    const value = result(["v"], [[1]]);
    edit(value);
    assert.equal(tryEncodeQueryResult(value), null);
  }
});

test("oversized sparse row counts fail before traversing their entries", () => {
  const value = {
    columns: [],
    columnCount: 0,
    columnTypes: [],
    rows: new Array(1_000_001),
    rowArrays: new Array(1_000_001),
    changes: 0,
  };
  Object.defineProperty(value.rows, "0", {
    get() {
      assert.fail("must reject shape before walking rows");
    },
  });
  assert.equal(tryEncodeQueryResult(value), null);
});

const malformed = (value) => assert.throws(() => decodeQueryResult(value), ResultCodecError);
for (const [field, offset, bits, value] of [
  ["magic", 0, 32, 0],
  ["version", 4, 16, 2],
  ["reserved flags", 6, 16, 1],
  ["length", 8, 32, 1],
  ["row bomb", 12, 32, 0xffffffff],
  ["column bomb", 16, 32, 0xffffffff],
  ["type bomb", 20, 32, 0xffffffff],
]) {
  test(`decoder rejects malformed ${field}`, () => {
    const frame = tryEncodeQueryResult(result(["v"], [[1]]));
    new DataView(frame)[`setUint${bits}`](offset, value, true);
    malformed(frame);
  });
}

test("decoder rejects invalid scalar tags, lengths, incomplete frames and trailing bytes", () => {
  const original = tryEncodeQueryResult(result(["v"], [[1]]));
  const tag = original.slice(0);
  new Uint8Array(tag)[38] = 255;
  malformed(tag);
  for (let n = 0; n < original.byteLength; n++) malformed(original.slice(0, n));
  const text = tryEncodeQueryResult(result(["v"], [["a"]]));
  new DataView(text).setUint32(39, 0xffffffff, true);
  malformed(text);
  const blob = tryEncodeQueryResult(result(["v"], [[Uint8Array.of(1)]]));
  new DataView(blob).setUint32(39, 0xffffffff, true);
  malformed(blob);
  const trailing = new Uint8Array(original.byteLength + 1);
  trailing.set(new Uint8Array(original));
  new DataView(trailing.buffer).setUint32(8, trailing.byteLength, true);
  malformed(trailing.buffer);
});

test("decoder rejects detached, shared and invalid buffers plus invalid row-count metadata", () => {
  const frame = tryEncodeQueryResult(result(["v"], [[1]]));
  structuredClone(frame, { transfer: [frame] });
  malformed(frame);
  malformed(new SharedArrayBuffer(64));
  malformed({ byteLength: 64 });
  const changes = tryEncodeQueryResult(result([], []));
  new DataView(changes).setFloat64(24, NaN, true);
  malformed(changes);
});

test("result-encoding options reject invalid values before use", () => {
  assert.equal(resolveResultEncoding(), "structured-clone");
  for (const mode of ["binary", "auto", "structured-clone"])
    assert.equal(resolveResultEncoding(mode), mode);
  for (const value of [null, false, "", "fast", {}, []])
    assert.throws(() => resolveResultEncoding(value), { code: "ERR_FSQLITE_RESULT_ENCODING" });
});

test("deterministic mixed-type corpus roundtrips through real structured-clone transfer", () => {
  let seed = 1729;
  const random = () => (seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0);
  for (let trial = 0; trial < 100; trial++) {
    const columns = Array.from({ length: 1 + (random() % 12) }, (_, i) => `c${i}`);
    const values = () => {
      const n = random();
      switch (n % 7) {
        case 0:
          return null;
        case 1:
          return Boolean(n & 8);
        case 2:
          return (n - 0x7fffffff) / 17;
        case 3:
          return BigInt(n) * 1000000000n;
        case 4:
          return String.fromCharCode(n % 65536).repeat(n % 37);
        case 5:
          return Uint8Array.from({ length: n % 61 }, () => random() % 256);
        default:
          return -0;
      }
    };
    const value = result(
      columns,
      Array.from({ length: random() % 41 }, () => columns.map(values)),
    );
    const encoded = tryEncodeQueryResult(value);
    assert.deepEqual(decodeQueryResult(structuredClone(encoded, { transfer: [encoded] })), value);
  }
});
