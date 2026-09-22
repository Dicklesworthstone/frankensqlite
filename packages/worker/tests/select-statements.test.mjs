// From the repository root:
// node --experimental-loader=./packages/sdk/tests/helpers/source-loader.mjs \
//   --test packages/worker/tests/select-statements.test.mjs
// Production classifier and managed batch execution; Node SQLite is the SQL
// oracle, not evidence of native/WASM conformance or a SQL security sandbox.
import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import test from "node:test";
import {
  executeManagedBatch,
  isSelectStatement,
  validateManagedSql,
} from "../src/transactions.ts";

const reads = [
  ["plain", "SELECT 42 AS n"],
  ["ordinary", "WITH x AS (SELECT 42 AS n) SELECT n FROM x"],
  ["column list", "WITH x(n) AS (SELECT 42) SELECT n FROM x"],
  ["multiple columns", "WITH x(a,b) AS (VALUES(40,2)) SELECT a+b AS n FROM x"],
  ["recursive", "WITH RECURSIVE x(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM x WHERE n<42) SELECT max(n) AS n FROM x"],
  ["implicit recursive", "WITH x(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM x WHERE n<42) SELECT max(n) AS n FROM x"],
  ["multiple CTEs", "WITH x(n) AS (SELECT 40), y(n) AS (SELECT n+2 FROM x) SELECT n FROM y"],
  ["materialized", "WITH x(n) AS MATERIALIZED (SELECT 42) SELECT n FROM x"],
  ["not materialized", "WITH x(n) AS NOT MATERIALIZED (SELECT 42) SELECT n FROM x"],
  ["nested CTE", "WITH x AS (WITH y AS (SELECT 42 AS n) SELECT n FROM y) SELECT n FROM x"],
  ["nested expression", "WITH x(n) AS (SELECT ((40)+(SELECT 2))) SELECT n FROM x"],
  ["compound", "WITH x(n) AS (SELECT 42) SELECT n FROM x UNION ALL SELECT 0 WHERE 0"],
  ["comments", "/*WITH DELETE*/ WITH /*a*/ x /*b*/ (n) AS /*c*/ (SELECT 42 /*) SELECT;*/) -- ) DELETE\n SELECT n FROM x"],
  ["case and BOM", "\uFEFF;; wItH x(n) aS (sElEcT 42) SeLeCt n FROM x;; -- end"],
  ["quoted keyword", 'WITH "DELETE"("SELECT") AS (VALUES(42)) SELECT "SELECT" AS n FROM "DELETE"'],
  ["bracket name", "WITH [x) SELECT;](n) AS (SELECT 42) SELECT n FROM [x) SELECT;]"],
  ["backtick escape", "WITH `x``)DELETE`(n) AS (SELECT 42) SELECT n FROM `x``)DELETE`"],
  ["single quote name", "WITH 'x''SELECT'(n) AS (SELECT 42) SELECT n FROM 'x''SELECT'"],
  ["quoted recursive", 'WITH "recursive"(n) AS (SELECT 42) SELECT n FROM "recursive"'],
  ["unicode and dollar", "WITH 名$称(n) AS (SELECT 42) SELECT n FROM 名$称"],
  ["string delimiters", "WITH x(n,s) AS (SELECT 42, ') SELECT; '' /*') SELECT n FROM x"],
  ["trailing comment", "WITH x AS (SELECT 42 AS n) SELECT n FROM x /* unfinished"],
];
for (const [name, sql] of reads) {
  test(`SELECT admission and SQLite execution: ${name}`, () => {
    assert.equal(isSelectStatement(sql), true);
    const db = new DatabaseSync(":memory:");
    try {
      assert.equal(db.prepare(sql).get().n, 42);
    } finally {
      db.close();
    }
  });
}

for (const sigil of ["$", ":", "@"]) {
  test(`Tcl ${sigil} parameter parentheses and quotes remain opaque`, () => {
    const name = `${sigil}n::part(foo;SELECT;('bar)`;
    const sql = `WITH x(n) AS (SELECT ${name}) SELECT n FROM x`;
    assert.equal(isSelectStatement(sql), true);
    assert.equal(isSelectStatement(`WITH x(n) AS (SELECT ${name}) DELETE FROM data`), false);
    const db = new DatabaseSync(":memory:");
    try {
      assert.equal(db.prepare(sql).get({ [name]: 42 }).n, 42);
    } finally {
      db.close();
    }
  });
}

const writes = [
  "INSERT INTO data VALUES(2,9)",
  "REPLACE INTO data VALUES(1,9)",
  "UPDATE data SET n=9",
  "DELETE FROM data",
];
const prefixes = [
  "",
  "WITH x AS (SELECT 1) ",
  'WITH "SELECT"(n) AS MATERIALIZED (SELECT 1), y AS NOT MATERIALIZED (SELECT n FROM "SELECT") ',
  "WITH RECURSIVE x(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM x WHERE n<3) ",
  "WITH x AS (WITH y AS (SELECT ') SELECT;') SELECT * FROM y) ",
];
for (const prefix of prefixes) {
  for (const write of writes) {
    test(`refuses actual SQLite mutation: ${prefix}${write}`, () => {
      const sql = `${prefix}${write} RETURNING n`;
      assert.equal(isSelectStatement(sql), false);
      // Negative control: these are executable writes returning rows, not
      // invalid syntax or a paired error that could pass without the fence.
      const db = new DatabaseSync(":memory:");
      try {
        db.exec("CREATE TABLE data(id INTEGER PRIMARY KEY,n); INSERT INTO data VALUES(1,0)");
        const before = db.prepare("SELECT * FROM data").all();
        assert.ok(db.prepare(sql).all().length > 0);
        assert.notDeepEqual(db.prepare("SELECT * FROM data").all(), before);
      } finally {
        db.close();
      }
    });
  }
}

for (const sql of [
  "PRAGMA user_version=1", "EXPLAIN SELECT 1", "ATTACH ':memory:' AS other",
  "CREATE TABLE data(n)", "VALUES(42)", '"SELECT" 1', "SELECT$not_keyword 1",
  "WITH", "WITH x", "WITH x SELECT 1", "WITH x AS SELECT 1",
  "WITH x() AS (SELECT 1) SELECT 1", "WITH x(a,) AS (SELECT 1) SELECT 1",
  "WITH x AS NOT (SELECT 1) SELECT 1", "WITH x AS MATERIALIZED SELECT 1",
  "WITH x AS () SELECT 1", "WITH x AS (DELETE FROM data) SELECT 1",
  "WITH x AS (SELECT 1 SELECT 1", "WITH x AS (SELECT 1)) SELECT 1",
  "WITH x AS (SELECT 1), SELECT 1", "WITH x AS (SELECT 1) 'SELECT' 1",
]) {
  test(`unsupported or incomplete prefix is not a SELECT: ${sql}`, () => {
    assert.equal(isSelectStatement(sql), false);
  });
}

for (const sql of [
  "", ";; /*empty*/", "BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT x", "RELEASE x",
  "SELECT 1; DELETE FROM data", "WITH x AS (SELECT 1) SELECT 1; UPDATE data SET n=9",
  "WITH x AS (SELECT 1; DELETE FROM data) SELECT 1", "SELECT 'unterminated",
  "SELECT $x(white space)", "SELECT 1 /*\0*/", "WITH x AS (SELECT '\0') SELECT 1",
]) {
  test(`complete preflight rejects before SELECT admission: ${JSON.stringify(sql)}`, () => {
    assert.throws(() => isSelectStatement(sql));
    assert.throws(() => validateManagedSql(sql));
  });
}

test("deep CTE expressions use bounded parser state, not the JS call stack", () => {
  const body = `SELECT ${"(".repeat(10_000)}1${")".repeat(10_000)}`;
  assert.equal(isSelectStatement(`WITH x AS (${body}) SELECT * FROM x`), true);
  assert.equal(isSelectStatement(`WITH x AS (${body}) DELETE FROM data`), false);
});

test("shared scanner preserves managed trigger bodies, scripts and full preflight", async () => {
  const sql = `CREATE TABLE data(n); CREATE TEMP TRIGGER t AFTER INSERT ON data BEGIN
    UPDATE data SET n=CASE WHEN n=1 THEN 42 ELSE n END;
    SELECT ') SELECT; END;'; END; INSERT INTO data VALUES(1);`;
  assert.equal(isSelectStatement("CREATE TRIGGER t AFTER INSERT ON data BEGIN SELECT 1; END;"), false);
  const db = new DatabaseSync(":memory:");
  const calls = [];
  try {
    await executeManagedBatch({ async executeBatch(part) { calls.push(part); db.exec(part); } }, sql, () => {});
    assert.equal(calls.length, 3);
    assert.equal(db.prepare("SELECT n FROM data").get().n, 42);
    let effects = 0;
    await assert.rejects(executeManagedBatch({ async executeBatch() { effects++; } }, "SELECT 1; COMMIT", () => {}));
    assert.equal(effects, 0);
  } finally {
    db.close();
  }
});
