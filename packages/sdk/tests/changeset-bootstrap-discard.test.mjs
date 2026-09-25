import assert from 'node:assert/strict';
import { test } from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  ChangesetBootstrapReceiver,
  CHANGESET_BOOTSTRAP_PROTOCOL,
  CHANGESET_BOOTSTRAP_STATE_TABLE,
  CHANGESET_BOOTSTRAP_CHUNKS_TABLE,
} from '../src/changeset-bootstrap.ts';

const S = `main."${CHANGESET_BOOTSTRAP_STATE_TABLE}"`;
const C = `main."${CHANGESET_BOOTSTRAP_CHUNKS_TABLE}"`;
const manifest = (extra = {}) => ({
  protocol: CHANGESET_BOOTSTRAP_PROTOCOL,
  receiverId: 'replica-42', deliveryId: 'source:seed', tables: ['t'],
  chunks: 2, changes: 2, byteLength: 8, sha256: 'a'.repeat(64), ...extra,
});
// Persisted-state fixtures exercise discard independently of upload/apply. The
// SQL engine, transaction rollback and competing file connections are real.
class Target {
  constructor(path = ':memory:') {
    this.db = new DatabaseSync(path);
    this.db.exec('PRAGMA journal_mode=WAL; CREATE TABLE IF NOT EXISTS t(id INTEGER PRIMARY KEY, value);');
    this.sql = [];
  }
  async execute(sql, params = []) {
    this.sql.push(sql);
    const changes = Number(this.db.prepare(sql).run(...params).changes);
    await this.afterExecute?.(sql);
    return this.overrideCount?.(sql, changes) ?? changes;
  }
  async query(sql, params = []) {
    this.sql.push(sql);
    const statement = this.db.prepare(sql);
    statement.setReadBigInts(true); statement.setReturnArrays(true);
    const rowArrays = statement.all(...params);
    await this.afterQuery?.(sql);
    return { rowArrays };
  }
  async transaction(work) {
    this.db.exec('BEGIN');
    let committed = false;
    try {
      const result = await work(this);
      await this.beforeCommit?.();
      this.db.exec('COMMIT'); committed = true;
      await this.afterCommit?.();
      return result;
    } catch (error) {
      if (!committed) this.db.exec('ROLLBACK');
      throw error;
    }
  }
  close() { this.db.close(); }
}
function fixture(target, m = manifest(), options = {}) {
  target.db.exec(`CREATE TABLE ${S} (id INTEGER PRIMARY KEY, manifest TEXT NOT NULL,
    received INTEGER NOT NULL, bytes INTEGER NOT NULL, changes INTEGER NOT NULL,
    chain TEXT NOT NULL, installed INTEGER NOT NULL);
    CREATE TABLE ${C} (idx INTEGER PRIMARY KEY, sha256 TEXT NOT NULL,
    byte_length INTEGER NOT NULL, change_count INTEGER NOT NULL, payload BLOB NOT NULL);`);
  const received = options.received ?? 1;
  const stored = options.orderedSourceId === undefined ? m : { ...m, orderedSourceId: options.orderedSourceId };
  target.db.prepare(`INSERT INTO ${S} VALUES(1,?,?,?,?,?,?)`).run(
    JSON.stringify(stored), received, received * 4, received,
    received === m.chunks ? m.sha256 : 'b'.repeat(64), Number(options.installed ?? false),
  );
  for (let i = 0; i < received; i++) {
    target.db.prepare(`INSERT INTO ${C} VALUES(?,?,4,1,?)`).run(
      i, 'c'.repeat(64), new Uint8Array(options.installed ? [] : [1, 2, 3, 4]),
    );
  }
}
function receiver(target, options = {}) {
  return new ChangesetBootstrapReceiver(target, {
    receiverId: 'replica-42', tables: ['t'], confirmCommit: async () => {}, ...options,
  });
}
function retained(target) {
  return [target.db.prepare(`SELECT count(*) AS n FROM ${S}`).get().n,
    target.db.prepare(`SELECT count(*) AS n FROM ${C}`).get().n];
}
const code = kind => error => error?.code === `ERR_FSQLITE_BOOTSTRAP_${kind}`;

test('discard removes the exact partial group, preserves schema and application rows', async t => {
  const target = new Target(); t.after(() => target.close()); fixture(target);
  target.db.exec("INSERT INTO t VALUES(9,'unrelated')");
  let confirmations = 0;
  const r = receiver(target, { confirmCommit: async () => { confirmations++; } });
  assert.equal(await r.discard(manifest()), true);
  assert.deepEqual(retained(target), [0, 0]);
  assert.equal(target.db.prepare('SELECT value FROM t WHERE id=9').get().value, 'unrelated');
  assert.equal(await r.status(manifest()), null);
  assert.equal(await r.discard(manifest()), false);
  assert.equal(confirmations, 2);
  assert.ok(!target.sql.some(sql => /DROP|SELECT payload/i.test(sql)));
});
test('discard on a fresh database creates no reserved tables and still confirms', async t => {
  const target = new Target(); t.after(() => target.close()); let calls = 0;
  assert.equal(await receiver(target, { confirmCommit: async () => { calls++; } }).discard(manifest()), false);
  assert.equal(calls, 1);
  assert.deepEqual(target.db.prepare("SELECT name FROM sqlite_schema WHERE name LIKE '__fsqlite_%'").all(), []);
});
test('complete but uninstalled uploads may be explicitly discarded', async t => {
  const target = new Target(); t.after(() => target.close()); fixture(target, manifest(), { received: 2 });
  assert.equal(await receiver(target).discard(manifest()), true);
  assert.deepEqual(retained(target), [0, 0]);
});
test('installed decisions and their chunk tombstones can never be discarded', async t => {
  const target = new Target(); t.after(() => target.close());
  fixture(target, manifest(), { received: 2, installed: true });
  target.db.exec("INSERT INTO t VALUES(1,'installed')");
  let confirmed = false;
  await assert.rejects(receiver(target, { confirmCommit: async () => { confirmed = true; } }).discard(manifest()), code('STATE'));
  assert.deepEqual(retained(target), [1, 2]); assert.equal(confirmed, false);
  assert.equal(target.db.prepare('SELECT value FROM t').get().value, 'installed');
});
for (const [name, extra, kind] of [
  ['delivery identity', { deliveryId: 'other' }, 'STATE'],
  ['digest', { sha256: 'f'.repeat(64) }, 'STATE'],
  ['counts', { chunks: 3 }, 'STATE'],
  ['recipient', { receiverId: 'other' }, 'INPUT'],
  ['table authority', { tables: ['other'] }, 'INPUT'],
  ['protocol', { protocol: 'other' }, 'INPUT'],
]) test(`discard refuses a different ${name}`, async t => {
  const target = new Target(); t.after(() => target.close()); fixture(target);
  await assert.rejects(receiver(target).discard(manifest(extra)), code(kind));
  assert.deepEqual(retained(target), [1, 1]);
});
test('discard retains the stored ordered-source policy across reopen', async t => {
  const target = new Target(); t.after(() => target.close());
  fixture(target, manifest(), { orderedSourceId: 'source-generation-1' });
  await assert.rejects(receiver(target).discard(manifest()), code('STATE'));
  await assert.rejects(receiver(target, { orderedSourceId: 'source-generation-2' }).discard(manifest()), code('STATE'));
  assert.equal(await receiver(target, { orderedSourceId: 'source-generation-1' }).discard(manifest()), true);
});
test('explicit discard can remove damaged payloads without loading or hashing them', async t => {
  const target = new Target(); t.after(() => target.close()); fixture(target);
  target.db.exec(`UPDATE ${C} SET payload=X'ff'`);
  assert.equal(await receiver(target).discard(manifest()), true);
  assert.deepEqual(retained(target), [0, 0]);
  assert.ok(!target.sql.some(sql => /^SELECT.*payload/i.test(sql)));
});
test('orphan chunks without an authoritative manifest are not discarded', async t => {
  const target = new Target(); t.after(() => target.close()); fixture(target);
  target.db.exec(`DELETE FROM ${S}`);
  await assert.rejects(receiver(target).discard(manifest()), code('CORRUPT'));
  assert.deepEqual(retained(target), [0, 1]);
});
test('partial storage schemas fail closed without deleting the existing object', async t => {
  const target = new Target(); t.after(() => target.close());
  target.db.exec(`CREATE TABLE ${S}(id INTEGER PRIMARY KEY)`);
  await assert.rejects(receiver(target).discard(manifest()), code('SCHEMA'));
  assert.equal(target.db.prepare("SELECT count(*) AS n FROM sqlite_schema WHERE name=?").get(CHANGESET_BOOTSTRAP_STATE_TABLE).n, 1);
});
test('storage triggers cannot turn discard into application writes', async t => {
  const target = new Target(); t.after(() => target.close()); fixture(target);
  target.db.exec(`CREATE TRIGGER dangerous AFTER DELETE ON ${CHANGESET_BOOTSTRAP_CHUNKS_TABLE} BEGIN INSERT INTO t VALUES(1,'changed'); END`);
  await assert.rejects(receiver(target).discard(manifest()), code('SCHEMA'));
  assert.deepEqual(retained(target), [1, 1]);
  assert.deepEqual(target.db.prepare('SELECT * FROM t').all(), []);
});
test('failure deleting chunks rolls the state deletion back', async t => {
  const target = new Target(); t.after(() => target.close()); fixture(target);
  const failure = new Error('injected SQL failure');
  target.afterExecute = sql => { if (sql === `DELETE FROM ${C}`) throw failure; };
  await assert.rejects(receiver(target).discard(manifest()), error => error === failure);
  assert.deepEqual(retained(target), [1, 1]);
});
test('invalid state-delete count aborts and restores the whole group', async t => {
  const target = new Target(); t.after(() => target.close()); fixture(target);
  target.overrideCount = (sql, count) => sql.startsWith(`DELETE FROM ${S}`) ? 0 : count;
  await assert.rejects(receiver(target).discard(manifest()), code('CORRUPT'));
  assert.deepEqual(retained(target), [1, 1]);
});
test('commit failure restores both staging tables', async t => {
  const target = new Target(); t.after(() => target.close()); fixture(target);
  const failure = new Error('commit refused'); target.beforeCommit = () => { throw failure; };
  await assert.rejects(receiver(target).discard(manifest()), error => error === failure);
  assert.deepEqual(retained(target), [1, 1]);
});
test('cancellation between deletes rolls back both tables', async t => {
  const target = new Target(); t.after(() => target.close()); fixture(target);
  const c = new AbortController();
  target.afterExecute = sql => { if (sql.startsWith(`DELETE FROM ${S}`)) c.abort(); };
  await assert.rejects(receiver(target).discard(manifest(), { signal: c.signal }), code('CANCELLED'));
  assert.deepEqual(retained(target), [1, 1]);
});
test('already-cancelled requests execute no SQL', async t => {
  const target = new Target(); t.after(() => target.close()); fixture(target);
  const c = new AbortController(); c.abort();
  await assert.rejects(receiver(target).discard(manifest(), { signal: c.signal }), code('CANCELLED'));
  assert.deepEqual(target.sql, []); assert.deepEqual(retained(target), [1, 1]);
});
test('confirmation failure after commit can be retried on absent state', async t => {
  const target = new Target(); t.after(() => target.close()); fixture(target);
  let calls = 0; const cause = new Error('checkpoint response lost');
  const r = receiver(target, { confirmCommit: async () => { if (++calls === 1) throw cause; } });
  await assert.rejects(r.discard(manifest()), error => code('CONFIRM')(error) && error.cause === cause);
  assert.deepEqual(retained(target), [0, 0]);
  assert.equal(await r.discard(manifest()), false); assert.equal(calls, 2);
});
test('lost SQL commit response recovers without inventing another manifest', async t => {
  const target = new Target(); t.after(() => target.close()); fixture(target);
  const cause = new Error('SQL response lost'); target.afterCommit = () => { throw cause; };
  await assert.rejects(receiver(target).discard(manifest()), error => error === cause);
  assert.deepEqual(retained(target), [0, 0]); target.afterCommit = undefined;
  assert.equal(await receiver(target).discard(manifest()), false);
});
test('cancellation during successful commit still drains confirmation', async t => {
  const target = new Target(); t.after(() => target.close()); fixture(target);
  const c = new AbortController(); let calls = 0; target.afterCommit = () => c.abort();
  await assert.rejects(receiver(target, { confirmCommit: async () => { calls++; } }).discard(manifest(), { signal: c.signal }), code('CANCELLED'));
  assert.equal(calls, 1); assert.deepEqual(retained(target), [0, 0]);
});
test('receiver admission stays occupied through discard confirmation', async t => {
  const target = new Target(); t.after(() => target.close()); fixture(target);
  let entered, release;
  const ready = new Promise(resolve => { entered = resolve; });
  const gate = new Promise(resolve => { release = resolve; });
  const r = receiver(target, { confirmCommit: async () => { entered(); await gate; } });
  const pending = r.discard(manifest()); await ready;
  await assert.rejects(r.discard(manifest()), code('BUSY'));
  await assert.rejects(r.status(manifest()), code('BUSY'));
  release(); assert.equal(await pending, true);
});
test('deadline expiry waits for an already-started confirmation', async t => {
  const target = new Target(); t.after(() => target.close()); fixture(target);
  let entered, release, settled = false;
  const ready = new Promise(resolve => { entered = resolve; });
  const gate = new Promise(resolve => { release = resolve; });
  const r = receiver(target, { confirmCommit: async () => { entered(); await gate; } });
  const pending = r.discard(manifest(), { timeoutMs: 1000 });
  const rejection = assert.rejects(pending, code('TIMEOUT')); pending.catch(() => { settled = true; });
  await ready; await new Promise(resolve => setTimeout(resolve, 1050));
  assert.equal(settled, false); release(); await rejection;
  assert.deepEqual(retained(target), [0, 0]);
});
test('competing installed decision cannot be erased by a stale discard snapshot', async t => {
  const path = join(mkdtempSync(join(tmpdir(), 'fsqlite-discard-race-')), 'db.sqlite');
  const target = new Target(path), other = new Target(path); t.after(() => { target.close(); other.close(); });
  fixture(target, manifest(), { received: 2 });
  let won = false;
  target.afterQuery = sql => {
    if (!won && sql.startsWith('SELECT id, CASE')) {
      won = true;
      // Commit the exact persisted installed state from an independent writer.
      // This test isolates the storage race, not the install/apply algorithm.
      other.db.exec(`BEGIN; UPDATE ${S} SET installed=1; UPDATE ${C} SET payload=X''; INSERT INTO t VALUES(1,'installed'); COMMIT;`);
    }
  };
  await assert.rejects(receiver(target).discard(manifest()), error => /locked|busy/i.test(error.message));
  assert.equal(won, true); assert.deepEqual(retained(target), [1, 2]);
  assert.equal(target.db.prepare(`SELECT installed FROM ${S}`).get().installed, 1);
  assert.equal(target.db.prepare('SELECT value FROM t').get().value, 'installed');
});
test('discard remains absent after file reopen while installed state remains protected', async t => {
  const path = join(mkdtempSync(join(tmpdir(), 'fsqlite-discard-reopen-')), 'db.sqlite');
  const original = new Target(path); fixture(original);
  assert.equal(await receiver(original).discard(manifest()), true); original.close();
  const reopened = new Target(path); t.after(() => reopened.close());
  assert.deepEqual(retained(reopened), [0, 0]);
  assert.equal(await receiver(reopened).discard(manifest()), false);
});
