import assert from 'node:assert/strict';
import { test } from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { createHash } from 'node:crypto';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';
import { ChangesetReceiver, CHANGESET_DELIVERY_PROTOCOL } from '../src/changeset-delivery.ts';
import { encodeChangeset } from '../src/changeset-codec.ts';

// Real SQLite SQL/transactions behind the structural SDK target, not a fake
// SQL interpreter. These tests do not certify FrankenSQLite Rust/WASM behavior.
class SqlTarget {
  constructor(path = ':memory:') {
    this.db = new DatabaseSync(path); this.sql = []; this.transactions = 0;
    this.db.exec('PRAGMA foreign_keys=ON; CREATE TABLE IF NOT EXISTS items(id INTEGER PRIMARY KEY, value TEXT)');
  }
  async execute(sql, params = []) {
    this.sql.push(sql); return Number(this.db.prepare(sql).run(...params).changes);
  }
  async query(sql, params = []) {
    this.sql.push(sql);
    const stmt = this.db.prepare(sql); stmt.setReadBigInts(true);
    return { rowArrays: stmt.all(...params).map(row => Object.values(row)) };
  }
  async transaction(work) {
    this.transactions++; this.db.exec('BEGIN'); let committed = false;
    try {
      const value = await work(this); this.db.exec('COMMIT'); committed = true;
      await this.afterCommit?.(); return this.resultOverride ?? value;
    } catch (error) {
      if (!committed) this.db.exec('ROLLBACK'); throw error;
    }
  }
}
const pause = ms => new Promise(resolve => setTimeout(resolve, ms));
function gate() {
  let release; const promise = new Promise(resolve => { release = resolve; });
  return { promise, release };
}
function payload(id = 1n, value = 'one') {
  return encodeChangeset([{ name: 'items', primaryKey: [1, 0],
    changes: [{ operation: 'insert', indirect: false, new: [id, value] }] }]);
}
function envelope(bytes = payload(), id = 'source:1') {
  return { protocol: CHANGESET_DELIVERY_PROTOCOL, receiverId: 'replica:A', deliveryId: id,
    sha256: createHash('sha256').update(bytes).digest('hex'), changeset: bytes };
}
function setup(t, options = {}, path) {
  const target = new SqlTarget(path); t.after(() => target.db.close());
  let confirmations = 0;
  const receiver = new ChangesetReceiver(target, { receiverId: 'replica:A', tables: ['items'],
    confirmCommit: async () => { confirmations++; }, ...options });
  return { target, receiver, confirmations: () => confirmations };
}
function rows(target) { return target.db.prepare('SELECT * FROM items ORDER BY id').all().map(row => ({ ...row })); }
function phase(expected) { return error => { assert.equal(error.phase, expected); return true; }; }

test('native session payload reaches SQL and acknowledgement binds exact bytes', async t => {
  const native = new DatabaseSync(':memory:');
  native.exec('CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)');
  const session = native.createSession(); t.after(() => { session.close(); native.close(); });
  native.exec("INSERT INTO items VALUES (1, 'native'), (2, 'second')");
  const message = envelope(session.changeset());
  const s = setup(t);
  const ack = await s.receiver.receive(message);
  assert.deepEqual(ack, { protocol: CHANGESET_DELIVERY_PROTOCOL, receiverId: 'replica:A', deliveryId: 'source:1',
    sha256: message.sha256, byteLength: message.changeset.length, applied: 2, omitted: 0, replayed: false, confirmed: true });
  assert.equal(Object.isFrozen(ack), true); assert.equal(s.confirmations(), 1);
  assert.deepEqual(rows(s.target), [{ id: 1, value: 'native' }, { id: 2, value: 'second' }]);
});

test('confirmation failure leaves a replayable receipt, never a success ACK', async t => {
  let confirmations = 0;
  const s = setup(t, { confirmCommit: async () => { if (++confirmations === 1) throw new Error('checkpoint failed'); } });
  await assert.rejects(s.receiver.receive(envelope()), phase('receiver-confirm'));
  assert.deepEqual(rows(s.target), [{ id: 1, value: 'one' }]);
  const before = s.target.sql.filter(sql => sql.startsWith('INSERT OR ABORT INTO main."items"')).length;
  const ack = await s.receiver.receive(envelope());
  assert.equal(ack.replayed, true); assert.equal(confirmations, 2);
  assert.equal(s.target.sql.filter(sql => sql.startsWith('INSERT OR ABORT INTO main."items"')).length, before);
});

test('lost SQL COMMIT response is recovered through the same identity', async t => {
  const s = setup(t); s.target.afterCommit = async () => { throw new Error('lost commit response'); };
  await assert.rejects(s.receiver.receive(envelope()), phase('receiver-apply'));
  assert.equal(s.confirmations(), 0); s.target.afterCommit = undefined;
  assert.equal((await s.receiver.receive(envelope())).replayed, true);
  assert.equal(s.confirmations(), 1); assert.equal(rows(s.target).length, 1);
});

test('lost successful receiver response never repeats row SQL or local conflict policy', async t => {
  let callbacks = 0;
  const s = setup(t, { onConflict: () => { callbacks++; return 'abort'; } });
  await s.receiver.receive(envelope()); // Deliberately discard the first response.
  assert.equal((await s.receiver.receive(envelope())).replayed, true);
  assert.equal(callbacks, 0); assert.equal(s.confirmations(), 2);
});

test('reusing an identity with different valid bytes rejects', async t => {
  const s = setup(t); await s.receiver.receive(envelope());
  await assert.rejects(s.receiver.receive(envelope(payload(2n))), phase('receiver-apply'));
  assert.equal(rows(s.target).length, 1); assert.equal(s.confirmations(), 1);
});

for (const [name, change] of [
  ['protocol', m => { m.protocol = 'other'; }], ['receiver', m => { m.receiverId = 'replica:B'; }],
  ['digest', m => { m.sha256 = '0'.repeat(64); }], ['digest syntax', m => { m.sha256 = 'X'.repeat(64); }],
  ['identity', m => { m.deliveryId = 'bad\0id'; }], ['invalid UTF-16', m => { m.deliveryId = '\ud800'; }],
  ['inherited field', m => { const id = m.deliveryId; delete m.deliveryId; Object.setPrototypeOf(m, { deliveryId: id }); }],
  ['accessor field', m => { Object.defineProperty(m, 'deliveryId', { get() { throw new Error('MUST NOT INVOKE'); } }); }],
  ['corrupt codec', m => { m.changeset = new Uint8Array([1]); m.sha256 = createHash('sha256').update(m.changeset).digest('hex'); }],
]) {
  test(`reject ${name} before any receiver SQL`, async t => {
    const s = setup(t), m = envelope(); change(m);
    await assert.rejects(s.receiver.receive(m), phase('validate'));
    assert.equal(s.target.transactions, 0); assert.equal(s.confirmations(), 0);
    assert.equal((await s.receiver.receive(envelope())).applied, 1); // Admission released.
  });
}

test('buffer admission rejects shared, resizable, detached and oversized bytes', async t => {
  const s = setup(t, { maxMessageBytes: 128 });
  const detached = new Uint8Array(0); structuredClone(detached, { transfer: [detached.buffer] });
  for (const bytes of [new Uint8Array(new SharedArrayBuffer(0)), new Uint8Array(new ArrayBuffer(0, { maxByteLength: 8 })), detached, new Uint8Array(129)]) {
    await assert.rejects(s.receiver.receive({ ...envelope(), changeset: bytes }), phase('validate'));
  }
  assert.equal(s.target.transactions, 0);
});

test('intrinsic buffer capture ignores hostile subclass iterators and length getters', async t => {
  class HostileBytes extends Uint8Array {
    get byteLength() { throw new Error('shadow getter'); }
    [Symbol.iterator]() { throw new Error('shadow iterator'); }
  }
  const bytes = payload(), s = setup(t), m = envelope(bytes);
  m.changeset = new HostileBytes(bytes);
  assert.equal((await s.receiver.receive(m)).applied, 1);
});

test('caller mutation after admission cannot change SQL or its acknowledged digest', async t => {
  const s = setup(t), m = envelope(), sha = m.sha256;
  const pending = s.receiver.receive(m); m.changeset.fill(0); m.sha256 = '0'.repeat(64); m.deliveryId = 'changed';
  const ack = await pending;
  assert.equal(ack.sha256, sha); assert.equal(ack.deliveryId, 'source:1');
  assert.deepEqual(rows(s.target), [{ id: 1, value: 'one' }]);
});

test('allowlist is captured and applied again on replay', async t => {
  const names = ['items']; const s = setup(t, { tables: names }); names[0] = 'other';
  await s.receiver.receive(envelope());
  const restricted = new ChangesetReceiver(s.target, { receiverId: 'replica:A', tables: [], confirmCommit: async () => assert.fail('unauthorized confirmation') });
  await assert.rejects(restricted.receive(envelope()), phase('receiver-apply'));
});

test('SQL conflicts roll back all rows and no receipt is confirmed', async t => {
  const s = setup(t); s.target.db.exec("INSERT INTO items VALUES (2,'existing')");
  const bytes = encodeChangeset([{ name: 'items', primaryKey: [1, 0], changes: [
    { operation: 'insert', indirect: false, new: [1n, 'one'] },
    { operation: 'insert', indirect: false, new: [2n, 'two'] },
  ] }]);
  await assert.rejects(s.receiver.receive(envelope(bytes)), phase('receiver-apply'));
  assert.deepEqual(rows(s.target), [{ id: 2, value: 'existing' }]); assert.equal(s.confirmations(), 0);
});

test('explicit omissions are retained and reconfirmed on replay', async t => {
  let policies = 0;
  const s = setup(t, { onConflict: () => { policies++; return 'omit'; } });
  s.target.db.exec("INSERT INTO items VALUES (1,'local')");
  const first = await s.receiver.receive(envelope()), again = await s.receiver.receive(envelope());
  assert.equal(first.applied, 0); assert.equal(first.omitted, 1); assert.equal(again.omitted, 1);
  assert.equal(again.replayed, true); assert.equal(policies, 1); assert.equal(s.confirmations(), 2);
});

test('deferred foreign-key failure cannot produce a confirmed receipt', async t => {
  const s = setup(t);
  s.target.db.exec('CREATE TABLE parent(id PRIMARY KEY); CREATE TABLE child(id PRIMARY KEY, p REFERENCES parent DEFERRABLE INITIALLY DEFERRED)');
  const receiver = new ChangesetReceiver(s.target, { receiverId: 'replica:A', tables: ['child'], confirmCommit: async () => assert.fail('COMMIT failed') });
  const bytes = encodeChangeset([{ name: 'child', primaryKey: [1, 0], changes: [{ operation: 'insert', indirect: false, new: [1n, 99n] }] }]);
  await assert.rejects(receiver.receive(envelope(bytes)), phase('receiver-apply'));
  assert.equal(s.target.db.prepare('SELECT count(*) n FROM child').get().n, 0);
});

test('same receiver rejects overlapping requests without growing a queue', async t => {
  const entered = gate(), finish = gate();
  const s = setup(t, { confirmCommit: async () => { entered.release(); await finish.promise; } });
  const first = s.receiver.receive(envelope()); await entered.promise;
  await assert.rejects(s.receiver.receive(envelope()), error => error.code === 'ERR_FSQLITE_DELIVERY_BUSY');
  assert.equal(s.target.transactions, 1); finish.release(); await first;
  assert.equal((await s.receiver.receive(envelope())).replayed, true);
});

test('cancelled confirmation is drained before rejection and later replay reconfirms', async t => {
  const entered = gate(), finish = gate(), controller = new AbortController(); let confirmations = 0, settled = false;
  const s = setup(t, { confirmCommit: async () => { confirmations++; entered.release(); await finish.promise; } });
  const first = s.receiver.receive(envelope(), { signal: controller.signal });
  const checked = assert.rejects(first, error => error.code === 'ERR_FSQLITE_DELIVERY_CANCELLED');
  first.then(() => { settled = true; }, () => { settled = true; });
  await entered.promise; controller.abort('stop'); await pause(10); assert.equal(settled, false);
  finish.release(); await checked;
  assert.equal((await s.receiver.receive(envelope())).replayed, true); assert.equal(confirmations, 2);
});

test('deadline does not abandon a running confirmation', async t => {
  const entered = gate(), finish = gate(); let settled = false;
  const s = setup(t, { confirmCommit: async () => { entered.release(); await finish.promise; } });
  const first = s.receiver.receive(envelope(), { timeoutMs: 100 });
  const checked = assert.rejects(first, error => error.code === 'ERR_FSQLITE_DELIVERY_TIMEOUT');
  first.then(() => { settled = true; }, () => { settled = true; });
  await entered.promise; await pause(120); assert.equal(settled, false);
  finish.release(); await checked;
});

test('already cancelled input never starts SQL or confirmation', async t => {
  const s = setup(t), controller = new AbortController(); controller.abort('already');
  await assert.rejects(s.receiver.receive(envelope(), { signal: controller.signal }), error => error.code === 'ERR_FSQLITE_DELIVERY_CANCELLED');
  assert.equal(s.target.transactions, 0); assert.equal(s.confirmations(), 0);
});

test('a malformed post-COMMIT adapter result is not acknowledged', async t => {
  const s = setup(t); s.target.resultOverride = { applied: 99, omitted: 0, replayed: false };
  await assert.rejects(s.receiver.receive(envelope()), error => error.code === 'ERR_FSQLITE_DELIVERY_RECEIPT');
  assert.equal(s.confirmations(), 0); s.target.resultOverride = undefined;
  assert.equal((await s.receiver.receive(envelope())).replayed, true);
});

test('receipt and receiver rows reopen together in a separate process', async t => {
  const path = join(mkdtempSync(join(tmpdir(), 'fsqlite-delivery-')), 'receiver.db');
  const s = setup(t, {}, path); await s.receiver.receive(envelope());
  const child = spawnSync(process.execPath, ['--input-type=module', '-e', `
    import { DatabaseSync } from 'node:sqlite';
    const db = new DatabaseSync(process.argv[1]);
    const row = db.prepare('SELECT value FROM items WHERE id=1').get();
    const receipt = db.prepare('SELECT applied, omitted FROM __fsqlite_changeset_receipts').get();
    console.log(JSON.stringify({row,receipt})); db.close();`, path], { encoding: 'utf8' });
  assert.equal(child.status, 0, child.stderr);
  assert.deepEqual(JSON.parse(child.stdout), { row: { value: 'one' }, receipt: { applied: 1, omitted: 0 } });
  const second = setup(t, {}, path); assert.equal((await second.receiver.receive(envelope())).replayed, true);
});

test('configuration rejects implicit confirmation and SDK/system table access', () => {
  for (const options of [{}, { confirmCommit: async () => {}, tables: ['__fsqlite_changeset_outbox'] },
    { confirmCommit: async () => {}, tables: ['sqlite_schema'] }, { confirmCommit: async () => {}, tables: ['items', 'ITEMS'] },
    { confirmCommit: async () => {}, maxMessageBytes: 0 }, { confirmCommit: async () => {}, maxMessageBytes: null }]) {
    assert.throws(() => new ChangesetReceiver({}, { receiverId: 'replica:A', tables: ['items'], ...options }));
  }
});

test('monotonic deadline reaches SQL checkpoints even when timer tasks are starved', async t => {
  const s = setup(t); const original = s.target.execute.bind(s.target);
  s.target.execute = async (sql, params) => {
    const value = await original(sql, params);
    if (sql.startsWith('INSERT OR ABORT INTO main."items"')) {
      const until = performance.now() + 120;
      while (performance.now() < until) { /* Deliberately starve timer tasks. */ }
    }
    return value;
  };
  await assert.rejects(s.receiver.receive(envelope(), { timeoutMs: 100 }), phase('receiver-apply'));
  assert.deepEqual(rows(s.target), []); assert.equal(s.confirmations(), 0);
});
