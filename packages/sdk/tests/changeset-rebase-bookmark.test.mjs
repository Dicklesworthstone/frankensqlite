// Production journal SQL on real Node SQLite. No browser/native-engine claim.
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { copyFileSync, mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import { encodeChangeset } from "../src/changeset-codec.ts";
import { ChangesetRebaser } from "../src/changeset-rebase.ts";
import {
  ChangesetRebaseJournal,
  REBASE_JOURNAL_ENTRIES_TABLE as ENTRIES,
  REBASE_JOURNAL_HEADS_TABLE as HEADS,
} from "../src/changeset-rebase-journal.ts";

const FORMAT = "fsqlite-rebase-bookmark-v1";
const EMPTY = new Uint8Array();
const code = (kind) => ({ code: `ERR_FSQLITE_REBASE_JOURNAL_${kind}` });
const options = (deliveryId, onConflict = () => "omit") => ({ tables: ["t"], deliveryId, onConflict });
const sha = (value) => createHash("sha256").update(value).digest("hex");
const jsonHash = (value) => sha(JSON.stringify(value));
const insert = (v) => encodeChangeset([{
  name: "t", primaryKey: [1, 0],
  changes: [{ operation: "insert", indirect: false, new: [1n, v] }],
}]);

function target(db) {
  let next = 0;
  const tx = {
    async execute(sql, params = []) { return Number(db.prepare(sql).run(...params).changes); },
    async query(sql, params = []) {
      const statement = db.prepare(sql); statement.setReadBigInts(true);
      return { rowArrays: statement.all(...params).map((row) => Object.values(row)) };
    },
  };
  return {
    admissions: 0,
    async transaction(work) {
      this.admissions++;
      const name = `bookmark_test_${++next}`;
      db.exec(`SAVEPOINT ${name}`);
      try {
        const result = await work(tx);
        db.exec(`RELEASE ${name}`);
        return result;
      } catch (error) {
        db.exec(`ROLLBACK TO ${name}; RELEASE ${name}`);
        throw error;
      }
    },
  };
}
function fixture(path = ":memory:", journalId = "history") {
  const db = new DatabaseSync(path);
  db.exec("CREATE TABLE IF NOT EXISTS t(k PRIMARY KEY,v)");
  const host = target(db), journal = new ChangesetRebaseJournal(host, { journalId });
  return { db, host, journal };
}
function intercept(host, read) {
  return { transaction: (work, options) => host.transaction((tx) => work({
    execute: (...args) => tx.execute(...args),
    async query(sql, params) {
      const result = await tx.query(sql, params);
      await read(sql, result);
      return result;
    },
  }), options) };
}
const state = (db) => JSON.stringify([
  db.prepare(`SELECT * FROM ${HEADS} ORDER BY journal_id`).all(),
  db.prepare(`SELECT * FROM ${ENTRIES} ORDER BY journal_id,position`).all(),
  db.prepare("SELECT * FROM __fsqlite_changeset_receipts ORDER BY delivery_id").all(),
]);

test("bookmark: empty identity is deterministic, serializable and read-only", async () => {
  const { db, journal } = fixture();
  try {
    const mark = await journal.bookmark();
    assert.deepEqual(mark, { format: FORMAT, journalId: "history", position: 0, sha256: jsonHash([FORMAT, "history"]) });
    assert.equal(Object.isFrozen(mark), true);
    const result = await journal.rebase(EMPTY, { after: JSON.parse(JSON.stringify(mark)), through: mark });
    assert.deepEqual(result.afterBookmark, mark); assert.deepEqual(result.throughBookmark, mark);
    assert.equal(result.changeset.length, 0);
    assert.equal(db.prepare("SELECT count(*) AS n FROM sqlite_schema WHERE name LIKE '__fsqlite_%'").get().n, 0);
  } finally { db.close(); }
});

test("bookmark: empty decisions bind identity, order and all prefix metadata", async () => {
  const { db, journal } = fixture();
  try {
    let hash = jsonHash([FORMAT, "history"]);
    for (let i = 1; i <= 3; i++) {
      const result = await journal.apply(EMPTY, options(`peer:${i}`));
      const e = result.entry;
      hash = jsonHash([FORMAT, hash, i, e.deliveryId, e.messageSha256, e.messageBytes, e.sha256, e.rebaseInfo.length]);
      const mark = await journal.bookmark();
      assert.equal(mark.position, i); assert.equal(mark.sha256, hash);
      await journal.apply(EMPTY, options(`peer:${i}`));
      assert.deepEqual(await journal.bookmark(), mark, "receipt replay cannot advance history");
    }
  } finally { db.close(); }
});

test("bookmark: real file backup forks reject same positions and an identical final entry", async () => {
  const directory = mkdtempSync(join(tmpdir(), "fsqlite-bookmarks-"));
  const original = join(directory, "original.db"), restored = join(directory, "restored.db");
  let a = fixture(original), b;
  try {
    await a.journal.apply(EMPTY, options("common:1"));
    const common = await a.journal.bookmark();
    a.db.close(); a = null;
    copyFileSync(original, restored);
    a = fixture(original); b = fixture(restored);
    await a.journal.apply(EMPTY, options("branch:a"));
    await b.journal.apply(EMPTY, options("branch:b"));
    await a.journal.apply(EMPTY, options("same:last"));
    await b.journal.apply(EMPTY, options("same:last"));
    assert.deepEqual(await a.journal.read("same:last"), await b.journal.read("same:last"));
    assert.deepEqual(await a.journal.head(), await b.journal.head());
    const bookmark = await a.journal.bookmark(), before = state(b.db);
    assert.notEqual(bookmark.sha256, (await b.journal.bookmark()).sha256);
    await assert.rejects(b.journal.rebase(EMPTY, { after: bookmark }), code("HISTORY"));
    await assert.rejects(b.journal.rebase(EMPTY, { through: bookmark }), code("HISTORY"));
    await assert.rejects(b.journal.rebase(EMPTY, { after: bookmark, through: bookmark }), code("HISTORY"));
    assert.equal(state(b.db), before);
    const shared = await b.journal.rebase(EMPTY, { after: common });
    assert.deepEqual(shared.afterBookmark, common); assert.equal(shared.through, 3);
    a.db.close(); a = fixture(original);
    assert.deepEqual(await a.journal.bookmark(), bookmark, "fresh reopen retains prefix identity");
    assert.equal(a.db.prepare("PRAGMA integrity_check").get().integrity_check, "ok");
    assert.equal(b.db.prepare("PRAGMA integrity_check").get().integrity_check, "ok");
  } finally { a?.db.close(); b?.db.close(); }
});

for (const kind of ["message", "decision", "journal-id"]) {
  test(`bookmark: same sequence rejects different ${kind}`, async () => {
    const a = fixture(), b = fixture(":memory:", kind === "journal-id" ? "other" : "history");
    try {
      if (kind === "decision") {
        a.db.exec("INSERT INTO t VALUES(1,'local')"); b.db.exec("INSERT INTO t VALUES(1,'local')");
      }
      await a.journal.apply(insert("a"), options("same:1", () => "omit"));
      await b.journal.apply(insert(kind === "message" ? "b" : "a"), options("same:1", () => kind === "decision" ? "replace" : "omit"));
      const mark = await a.journal.bookmark();
      assert.notEqual(mark.sha256, (await b.journal.bookmark()).sha256);
      await assert.rejects(b.journal.rebase(EMPTY, { after: mark }), code("HISTORY"));
    } finally { a.db.close(); b.db.close(); }
  });
}

test("bookmark: absent older backup tip is missing, never latest-state fallback", async () => {
  const a = fixture(), b = fixture();
  try {
    await a.journal.apply(EMPTY, options("one"));
    await assert.rejects(b.journal.rebase(EMPTY, { through: await a.journal.bookmark() }), code("MISSING"));
    assert.equal((await b.journal.bookmark()).position, 0);
  } finally { a.db.close(); b.db.close(); }
});

for (const endpoint of ["after", "through"]) for (const populated of [false, true]) {
  test(`bookmark: valid-shaped wrong digest at ${endpoint}, populated=${populated}`, async () => {
    const { db, journal } = fixture();
    try {
      if (populated) await journal.apply(EMPTY, options("one"));
      const mark = { ...await journal.bookmark(), sha256: "0".repeat(64) };
      await assert.rejects(journal.rebase(EMPTY, { [endpoint]: mark }), code("HISTORY"));
    } finally { db.close(); }
  });
}

test("bookmark: valid ranges preserve rebase semantics and return both snapshot identities", async () => {
  const { db, journal } = fixture();
  db.exec("INSERT INTO t VALUES(1,'local')");
  try {
    const local = insert("local"), marks = [await journal.bookmark()], infos = [];
    for (let i = 0; i < 3; i++) {
      infos.push((await journal.apply(insert(`remote:${i}`), options(`r${i}`, () => i === 1 ? "replace" : "omit"))).entry.rebaseInfo);
      marks.push(await journal.bookmark());
    }
    for (let after = 0; after <= 3; after++) for (let through = after; through <= 3; through++) {
      const rebaser = new ChangesetRebaser();
      for (const info of infos.slice(after, through)) rebaser.configure(info);
      const result = await journal.rebase(local, { after: marks[after], through: marks[through] });
      assert.deepEqual(result.changeset, rebaser.rebase(local));
      assert.deepEqual(result.afterBookmark, marks[after]); assert.deepEqual(result.throughBookmark, marks[through]);
      assert.equal(Object.isFrozen(result.afterBookmark), true); assert.equal(Object.isFrozen(result.throughBookmark), true);
      const numeric = await journal.rebase(local, { after, through });
      assert.deepEqual(numeric, result);
    }
    await assert.rejects(journal.rebase(local, { after: marks[2], through: marks[1] }), code("MISSING"));
  } finally { db.close(); }
});

for (const [field, value] of [
  ["format", "future"], ["journalId", ""], ["journalId", "\ud800"], ["position", -1],
  ["position", 1.5], ["position", 100001], ["position", NaN], ["position", 1n],
  ["sha256", "a".repeat(63)], ["sha256", "A".repeat(64)], ["sha256", null],
]) {
  test(`bookmark: invalid ${field} ${String(value)} rejects before SQL admission`, async () => {
    const { db, host, journal } = fixture();
    try {
      const mark = { ...await journal.bookmark(), [field]: value }, before = host.admissions;
      await assert.rejects(journal.rebase(EMPTY, { after: mark }), code("INPUT"));
      assert.equal(host.admissions, before);
    } finally { db.close(); }
  });
}
for (const field of ["format", "journalId", "position", "sha256"]) {
  test(`bookmark: ${field} accessors are refused without invocation`, async () => {
    const { db, journal } = fixture();
    try {
      const mark = { ...await journal.bookmark() };
      let reads = 0;
      Object.defineProperty(mark, field, { get() { reads++; assert.fail("untrusted getter"); } });
      await assert.rejects(journal.rebase(EMPTY, { through: mark }), code("INPUT"));
      assert.equal(reads, 0);
    } finally { db.close(); }
  });
}

test("bookmark: fields and input bytes are captured before asynchronous admission", async () => {
  const { db, host, journal } = fixture();
  try {
    const actual = await journal.bookmark(), mark = { ...actual }, local = insert("original"), expected = Uint8Array.from(local);
    let release, entered;
    const gate = new Promise((resolve) => { release = resolve; });
    const arrival = new Promise((resolve) => { entered = resolve; });
    const delayed = { async transaction(work, opts) { entered(); await gate; return host.transaction(work, opts); } };
    const j = new ChangesetRebaseJournal(delayed, { journalId: "history" });
    const opts = { after: mark, through: mark };
    const pending = j.rebase(local, opts);
    await arrival;
    mark.sha256 = "0".repeat(64); mark.position = 90000; opts.after = 90000; local.fill(0);
    release();
    const result = await pending;
    assert.deepEqual(result.changeset, expected); assert.deepEqual(result.afterBookmark, actual);
  } finally { db.close(); }
});

for (const damage of [
  `UPDATE ${ENTRIES} SET sha256='${"0".repeat(64)}' WHERE position=1`,
  "DELETE FROM __fsqlite_changeset_receipts WHERE delivery_id='one'",
  `DELETE FROM ${ENTRIES} WHERE position=1`,
]) {
  test(`bookmark: excluded-prefix corruption is detected: ${damage.slice(0, 50)}`, async () => {
    const { db, journal } = fixture();
    try {
      await journal.apply(EMPTY, options("one")); await journal.apply(EMPTY, options("two"));
      const mark = await journal.bookmark(); db.exec(damage);
      const before = state(db);
      await assert.rejects(journal.bookmark(), code("CORRUPT"));
      await assert.rejects(journal.rebase(EMPTY, { after: mark, through: mark }), code("CORRUPT"));
      assert.equal(state(db), before);
    } finally { db.close(); }
  });
}

test("bookmark: cancellation inside prefix verification cannot publish a result", async () => {
  const { db, host, journal } = fixture();
  try {
    await journal.apply(EMPTY, options("one"));
    const c = new AbortController(), before = state(db);
    const hooked = intercept(host, (sql) => { if (sql.startsWith("SELECT position,")) c.abort(); });
    const j = new ChangesetRebaseJournal(hooked, { journalId: "history" });
    await assert.rejects(j.bookmark({ signal: c.signal }), code("CANCELLED"));
    assert.equal(state(db), before);
    assert.equal((await journal.bookmark()).position, 1);
  } finally { db.close(); }
});

test("bookmark: pre-cancelled operations enter no SQL", async () => {
  const { db, host, journal } = fixture();
  try {
    const c = new AbortController(); c.abort();
    await assert.rejects(journal.bookmark({ signal: c.signal }), code("CANCELLED"));
    await assert.rejects(journal.rebase(EMPTY, { signal: c.signal }), code("CANCELLED"));
    assert.equal(host.admissions, 0);
  } finally { db.close(); }
});

test("bookmark: expired monotonic budget stops a history scan", async (t) => {
  const { db, host, journal } = fixture();
  try {
    await journal.apply(EMPTY, options("one"));
    let now = 0;
    t.mock.method(performance, "now", () => now);
    const hooked = intercept(host, (sql) => { if (sql.startsWith("SELECT position,")) now = 20; });
    const j = new ChangesetRebaseJournal(hooked, { journalId: "history" });
    await assert.rejects(j.bookmark({ timeoutMs: 10 }), code("TIMEOUT"));
  } finally { db.close(); }
});

test("bookmark: a provisional child history cannot survive its outer rollback", async () => {
  const { db, host, journal } = fixture();
  let mark;
  try {
    await assert.rejects(host.transaction(async () => {
      await journal.apply(EMPTY, options("rolled-back"));
      mark = await journal.bookmark();
      throw new Error("outer rollback");
    }), /outer rollback/);
    assert.equal((await journal.bookmark()).position, 0);
    await assert.rejects(journal.rebase(EMPTY, { after: mark }), code("MISSING"));
  } finally { db.close(); }
});

test("bookmark: configured retention limits still apply before scanning", async () => {
  const { db, host, journal } = fixture();
  try {
    await journal.apply(EMPTY, options("one")); await journal.apply(EMPTY, options("two"));
    const bounded = new ChangesetRebaseJournal(host, { journalId: "history", maxEntries: 1 });
    await assert.rejects(bounded.bookmark(), code("LIMIT"));
    await assert.rejects(bounded.rebase(EMPTY), code("LIMIT"));
  } finally { db.close(); }
});

test("bookmark: results bind the read snapshot, not a later journal tip", async () => {
  const { db, host, journal } = fixture();
  try {
    await journal.apply(EMPTY, options("one"));
    const expected = await journal.bookmark();
    const later = { async transaction(work, opts) {
      const result = await host.transaction(work, opts);
      await journal.apply(EMPTY, options("later"));
      return result;
    } };
    const j = new ChangesetRebaseJournal(later, { journalId: "history" });
    const result = await j.rebase(EMPTY);
    assert.equal(result.through, 1); assert.deepEqual(result.throughBookmark, expected);
    assert.equal((await journal.bookmark()).position, 2);
    assert.deepEqual((await journal.rebase(EMPTY, { through: expected })).throughBookmark, expected);
  } finally { db.close(); }
});
