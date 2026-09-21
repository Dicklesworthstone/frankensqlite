// Native SQLite reference coverage for the production bulk executor.
// node --experimental-loader=./packages/sdk/tests/helpers/source-loader.mjs \
//   --test packages/worker/tests/bulk-bindings.test.mjs

import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import { BulkCancellation, BulkExecutionError, executeMany, validateBulkSql } from "../src/bulk.ts";

function fixture(t, bind = (values) => values) {
  const native = new DatabaseSync(":memory:");
  t.after(() => native.close());
  native.exec("CREATE TABLE t(a UNIQUE, b)");
  const calls = [];
  let frees = 0;
  const db = {
    async executeBatch(sql) {
      calls.push(sql);
      native.exec(sql);
    },
    async prepare(sql) {
      calls.push(`prepare: ${sql}`);
      const statement = native.prepare(sql);
      statement.setAllowBareNamedParameters(false);
      return {
        sql,
        columnCount: statement.columns().length,
        free() {
          frees++;
        },
        async executeWithParams(values) {
          calls.push("execute");
          return Number(statement.run(...bind(values)).changes);
        },
      };
    },
  };
  return {
    db,
    native,
    calls,
    get frees() {
      return frees;
    },
    rows() {
      return native
        .prepare("SELECT a,b FROM t ORDER BY a")
        .all()
        .map((row) => [row.a, row.b]);
    },
  };
}

for (const prepared of [false, true]) {
  for (const [label, bad] of [
    ["missing", [2]],
    ["extra", [2, "bad", 9]],
    ["empty", []],
  ]) {
    test(`${prepared ? "prepared" : "ad-hoc"} bulk ${label} bindings are rejected before any SQL`, async (t) => {
      const f = fixture(t),
        sql = "INSERT INTO t VALUES(?,?)";
      f.native.exec("INSERT INTO t VALUES(0,'keep'); BEGIN; INSERT INTO t VALUES(-1,'outer')");
      const handle = prepared ? await f.db.prepare(sql) : undefined;
      f.calls.length = 0;
      await assert.rejects(
        executeMany(f.db, sql, [[1, "first"], bad, [3, "last"]], "bulk_test", handle),
        (error) => {
          assert.ok(error instanceof BulkExecutionError);
          assert.equal(error.batchIndex, 1);
          assert.equal(error.cause.code, "ERR_FSQLITE_BINDING_ARITY");
          assert.equal(error.connectionUnusable, false);
          assert.deepEqual(error.cleanupErrors, []);
          return true;
        },
      );
      assert.deepEqual(f.calls, []);
      assert.equal(f.frees, 0, "caller-owned statements must survive input errors");
      assert.deepEqual(f.rows(), [
        [-1, "outer"],
        [0, "keep"],
      ]);
      const result = await executeMany(f.db, sql, [[4, "after"]], "bulk_after", handle);
      assert.deepEqual(result, { executions: 1, changes: 1, changesPerExecution: [1] });
      f.native.exec("ROLLBACK");
      assert.deepEqual(
        f.rows(),
        [[0, "keep"]],
        "the original outer transaction must still own its writes",
      );
      assert.equal(f.native.prepare("PRAGMA integrity_check").get().integrity_check, "ok");
    });
  }
}

for (const [label, value] of [
  ["undefined", undefined],
  ["object", {}],
  ["large bigint", 1n << 63n],
]) {
  test(`direct bulk rejects ${label} with the input row index and no SQL`, async (t) => {
    const f = fixture(t);
    await assert.rejects(
      executeMany(
        f.db,
        "INSERT INTO t VALUES(?,?)",
        [
          [1, "first"],
          [2, value],
        ],
        "bulk_test",
      ),
      (error) => {
        assert.equal(error.batchIndex, 1);
        assert.equal(error.cause.code, "ERR_FSQLITE_BINDING_INPUT");
        return true;
      },
    );
    assert.deepEqual(f.calls, []);
    assert.deepEqual(f.rows(), []);
  });
}

test("a sparse binding row is not silently converted to NULL", async (t) => {
  const f = fixture(t),
    row = new Array(2);
  row[0] = 1;
  await assert.rejects(
    executeMany(f.db, "INSERT INTO t VALUES(?,?)", [row], "bulk_test"),
    (error) => {
      assert.equal(error.batchIndex, 0);
      assert.equal(error.cause.code, "ERR_FSQLITE_BINDING_INPUT");
      return true;
    },
  );
  assert.deepEqual(f.calls, []);
});

for (const [label, sql, rows, bind, expected] of [
  [
    "explicit NULL",
    "INSERT INTO t VALUES(?,?)",
    [
      [1, null],
      [2, "ok"],
    ],
    (values) => values,
    [
      [1, null],
      [2, "ok"],
    ],
  ],
  [
    "zero parameters",
    "INSERT INTO t VALUES(1,'literal ? :x')",
    [[]],
    (values) => values,
    [[1, "literal ? :x"]],
  ],
  ["repeated name", "INSERT INTO t VALUES(:x,:x)", [[5]], ([x]) => [{ ":x": x }], [[5, 5]]],
  [
    "numbered holes",
    "INSERT INTO t VALUES(?3,?1)",
    [[7, null, 9]],
    (values) => [{ "?1": values[0], "?3": values[2] }],
    [[9, 7]],
  ],
  ["numbered aliases", "INSERT INTO t VALUES(?01,?1)", [[8]], ([x]) => [{ "?01": x }], [[8, 8]]],
  [
    "comments and quotes",
    `INSERT INTO t VALUES(?, '?9 :ignored') /* @ignored */`,
    [[6]],
    (values) => values,
    [[6, "?9 :ignored"]],
  ],
]) {
  test(`complete bulk bindings preserve native SQLite ${label}`, async (t) => {
    const f = fixture(t, bind);
    const result = await executeMany(f.db, sql, rows, "bulk_test");
    assert.equal(result.executions, rows.length);
    assert.equal(result.changes, rows.length);
    assert.deepEqual(f.rows(), expected);
    assert.equal(f.frees, 1);
  });
}

test("numbered holes require the full positional extent, not just used slots", async (t) => {
  const f = fixture(t);
  await assert.rejects(
    executeMany(f.db, "INSERT INTO t VALUES(?3,?1)", [[1, 3]], "bulk_test"),
    (error) => {
      assert.equal(error.batchIndex, 0);
      assert.equal(error.cause.code, "ERR_FSQLITE_BINDING_ARITY");
      return true;
    },
  );
  assert.deepEqual(f.calls, []);
});

for (const token of [
  "$a(semi;colon)",
  "$a(quote'comma,)",
  ":a::b",
  "@a(x;y)",
  "$::a(end;COMMIT;)",
]) {
  test(`bulk SQL preserves the opaque parameter token ${token}`, async (t) => {
    const f = fixture(t, ([value]) => [{ [token]: value }]);
    const sql = `INSERT INTO t VALUES(${token},'ok')`;
    const result = await executeMany(f.db, sql, [[11]], "bulk_test");
    assert.equal(result.changes, 1);
    assert.deepEqual(f.rows(), [[11, "ok"]]);
  });
}

for (const sql of [
  "INSERT INTO t VALUES($a(semi;colon),1); COMMIT",
  "INSERT INTO t VALUES($a(quote'),1); ROLLBACK",
  "INSERT INTO t VALUES(?,1); INSERT INTO t VALUES(2,2)",
  "INSERT INTO t VALUES(1,'nul\0')",
  "INSERT INTO t VALUES(1,1) -- nul\0",
  "INSERT INTO t VALUES($a(unclosed,1",
]) {
  test(`bulk boundary rejects ${JSON.stringify(sql)}`, () =>
    assert.throws(() => validateBulkSql(sql)));
}

test("dollar signs inside unquoted identifiers are not bind-token boundaries", async (t) => {
  const f = fixture(t);
  f.native.exec("CREATE TABLE table$dollars(a)");
  assert.equal(
    (await executeMany(f.db, "INSERT INTO table$dollars(a) VALUES(?)", [[1]], "bulk_test")).changes,
    1,
  );
});

test("native constraint failure still rolls back every row without poisoning an outer transaction", async (t) => {
  const f = fixture(t);
  f.native.exec("BEGIN; INSERT INTO t VALUES(0,'outer')");
  await assert.rejects(
    executeMany(
      f.db,
      "INSERT INTO t VALUES(?,?)",
      [
        [1, "one"],
        [1, "duplicate"],
      ],
      "bulk_test",
    ),
    (error) => {
      assert.equal(error.batchIndex, 1);
      assert.equal(error.connectionUnusable, false);
      return true;
    },
  );
  assert.deepEqual(f.rows(), [[0, "outer"]]);
  f.native.exec("COMMIT");
});

test("pre-cancellation and empty valid batches do not enter the core", async (t) => {
  const f = fixture(t),
    cancellation = new BulkCancellation();
  cancellation.request();
  await assert.rejects(
    executeMany(f.db, "INSERT INTO t VALUES(?,?)", [[1, 2]], "bulk_test", undefined, cancellation),
    { code: "ERR_FSQLITE_BULK_CANCELLED" },
  );
  assert.deepEqual(await executeMany(f.db, "INSERT INTO t VALUES(?,?)", [], "bulk_test"), {
    executions: 0,
    changes: 0,
    changesPerExecution: [],
  });
  assert.deepEqual(f.calls, []);
});
