import assert from 'node:assert/strict';
import { test } from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { createHash } from 'node:crypto';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';
import { ChangesetReceiver, ChangesetDeliveryPump, CHANGESET_DELIVERY_PROTOCOL } from '../src/changeset-delivery.ts';
import { ChangesetOutbox } from '../src/changeset-outbox.ts';
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

async function pair(t, options = {}) {
  const source = new SqlTarget(options.sourcePath); t.after(() => source.db.close());
  source.db.exec('PRAGMA recursive_triggers=ON');
  const outbox = new ChangesetOutbox(source);
  const destination = setup(t, options.receiverOptions, options.receiverPath);
  const pumpOptions = { receiverId: 'replica:A', confirmSource: async () => {},
    deliver: (message, controls) => destination.receiver.receive(message, controls) };
  const record = (sql, params, deliveryId) => outbox.record(tx => tx.execute(sql, params), { tables: ['items'], deliveryId });
  return { source, outbox, destination, pumpOptions, record,
    pump: overrides => new ChangesetDeliveryPump(outbox, { ...pumpOptions, ...overrides }) };
}
async function recordOne(p) { return p.record('INSERT INTO items VALUES (?,?)', [1n, 'one'], 'source:1'); }
function adapt(outbox, overrides = {}) {
  return { pending: options => outbox.pending(options), read: id => outbox.read(id),
    acknowledge: (id, sha) => outbox.acknowledge(id, sha), ...overrides };
}

test('capture -> retained outbox -> confirmed receiver SQL -> acknowledged source, in order', async t => {
  const p = await pair(t); await recordOne(p);
  await p.record('UPDATE items SET value=? WHERE id=?', ['two', 1n], 'source:2');
  await p.record('UPDATE items SET value=? WHERE id=?', ['three', 1n], 'source:3');
  const seen = [];
  const result = await p.pump({ deliver: async (m, o) => {
    seen.push(m.deliveryId);
    // Transport must never hold a source transaction open.
    p.source.db.exec('BEGIN; ROLLBACK');
    return p.destination.receiver.receive(m, o);
  } }).run();
  assert.deepEqual(seen, ['source:1', 'source:2', 'source:3']);
  assert.equal(result.deliveries, 3); assert.equal(result.applied, 3); assert.equal(result.stopped, 'empty');
  assert.equal(result.replays, 0); assert.equal(Object.isFrozen(result), true);
  assert.deepEqual(rows(p.source), rows(p.destination.target));
  assert.equal((await p.outbox.pending()).length, 0);
  for (const id of seen) assert.equal((await p.outbox.read(id)).changeset, null);
});

test('source and receiver confirmations bracket every delivery and acknowledgement', async t => {
  const events = [];
  const p = await pair(t, { receiverOptions: { confirmCommit: async () => { events.push('receiver-confirm'); } } });
  await recordOne(p);
  const outbox = adapt(p.outbox, {
    pending: async o => { events.push('pending'); return p.outbox.pending(o); },
    read: async id => { events.push('read'); return p.outbox.read(id); },
    acknowledge: async (id, sha) => { events.push('ack'); return p.outbox.acknowledge(id, sha); },
  });
  const pump = new ChangesetDeliveryPump(outbox, { ...p.pumpOptions,
    confirmSource: async () => { events.push('source-confirm'); },
    deliver: async (m, o) => { events.push('send'); return p.destination.receiver.receive(m, o); } });
  await pump.run();
  assert.deepEqual(events, ['source-confirm', 'pending', 'read', 'source-confirm', 'send', 'receiver-confirm', 'ack', 'source-confirm', 'pending']);
});

test('transport failure before submission preserves the oldest message and its successor', async t => {
  const p = await pair(t); await recordOne(p);
  await p.record('UPDATE items SET value=? WHERE id=?', ['two', 1n], 'source:2');
  let calls = 0;
  await assert.rejects(p.pump({ deliver: async () => { calls++; throw new Error('offline'); } }).run(), phase('transport'));
  assert.equal(calls, 1); assert.equal((await p.outbox.pending()).length, 2); assert.deepEqual(rows(p.destination.target), []);
  assert.equal((await p.pump().run()).deliveries, 2);
});

test('lost remote ACK keeps source bytes and replay never reapplies receiver writes', async t => {
  const p = await pair(t); await recordOne(p);
  await assert.rejects(p.pump({ deliver: async (m, o) => { await p.destination.receiver.receive(m, o); throw new Error('lost ACK'); } }).run(), phase('transport'));
  assert.equal((await p.outbox.pending()).length, 1); assert.notEqual((await p.outbox.read('source:1')).changeset, null);
  const before = p.destination.target.sql.filter(sql => sql.startsWith('INSERT OR ABORT INTO main."items"')).length;
  assert.equal((await p.pump().run()).replays, 1);
  assert.equal(p.destination.target.sql.filter(sql => sql.startsWith('INSERT OR ABORT INTO main."items"')).length, before);
});

for (const failedConfirmation of [1, 2]) {
  test(`source confirmation ${failedConfirmation} fails before any remote SQL`, async t => {
    const p = await pair(t); await recordOne(p); let confirmations = 0, sends = 0;
    await assert.rejects(p.pump({ confirmSource: async () => { if (++confirmations === failedConfirmation) throw new Error('source checkpoint'); },
      deliver: async () => { sends++; assert.fail('source unconfirmed'); } }).run(), phase('source-confirm'));
    assert.equal(sends, 0); assert.equal((await p.outbox.pending()).length, 1);
    assert.equal((await p.pump().run()).deliveries, 1);
  });
}

test('receiver confirmation failure cannot reclaim the source; replay confirms again', async t => {
  let confirmations = 0;
  const p = await pair(t, { receiverOptions: { confirmCommit: async () => { if (++confirmations === 1) throw new Error('receiver checkpoint'); } } });
  await recordOne(p);
  await assert.rejects(p.pump().run(), phase('transport'));
  assert.equal((await p.outbox.pending()).length, 1); assert.equal(rows(p.destination.target).length, 1);
  assert.equal((await p.pump().run()).replays, 1); assert.equal(confirmations, 2);
});

test('failed source acknowledgement retries delivery, never the original source callback', async t => {
  const p = await pair(t); let callbacks = 0;
  await p.outbox.record(async tx => { callbacks++; await tx.execute("INSERT INTO items VALUES(1,'one')"); }, { tables: ['items'], deliveryId: 'source:1' });
  const pump = new ChangesetDeliveryPump(adapt(p.outbox, { acknowledge: async () => { throw new Error('ack write failed'); } }), p.pumpOptions);
  await assert.rejects(pump.run(), phase('source-ack'));
  assert.equal((await p.outbox.pending()).length, 1);
  assert.equal((await p.pump().run()).replays, 1); assert.equal(callbacks, 1);
});

test('lost source acknowledgement response is recovered before observing an empty outbox', async t => {
  const p = await pair(t); await recordOne(p);
  const pump = new ChangesetDeliveryPump(adapt(p.outbox, { acknowledge: async (id, sha) => {
    await p.outbox.acknowledge(id, sha); throw new Error('lost source commit response');
  } }), p.pumpOptions);
  await assert.rejects(pump.run(), phase('source-ack'));
  assert.equal((await p.outbox.pending()).length, 0); let confirmations = 0;
  const result = await p.pump({ confirmSource: async () => { confirmations++; }, deliver: async () => assert.fail('already acknowledged') }).run();
  assert.equal(confirmations, 1); assert.equal(result.deliveries, 0); assert.equal(result.stopped, 'empty');
});

test('failed post-ACK source checkpoint cannot be bypassed by an empty pending list', async t => {
  const p = await pair(t); await recordOne(p); let confirmations = 0;
  await assert.rejects(p.pump({ confirmSource: async () => { if (++confirmations === 3) throw new Error('post-ack checkpoint'); } }).run(), phase('source-confirm'));
  assert.equal((await p.outbox.pending()).length, 0);
  await assert.rejects(p.pump({ confirmSource: async () => { throw new Error('still unresolved'); } }).run(), phase('source-confirm'));
  assert.equal((await p.pump().run()).stopped, 'empty');
});

for (const [name, mutate] of [
  ['protocol', a => { a.protocol = 'other'; }], ['receiver', a => { a.receiverId = 'wrong'; }],
  ['identity', a => { a.deliveryId = 'wrong'; }], ['digest', a => { a.sha256 = '0'.repeat(64); }],
  ['byte length', a => { a.byteLength++; }], ['confirmation', a => { a.confirmed = false; }],
  ['applied count', a => { a.applied++; }], ['omitted count', a => { a.omitted = -1; }],
  ['replay flag', a => { a.replayed = 1; }],
  ['accessor', a => { Object.defineProperty(a, 'confirmed', { get() { assert.fail('untrusted getter'); } }); }],
  ['inherited authority', a => { delete a.confirmed; Object.setPrototypeOf(a, { confirmed: true }); }],
]) {
  test(`reject mismatched ${name} receipt without acknowledging source`, async t => {
    const p = await pair(t); await recordOne(p);
    await assert.rejects(p.pump({ deliver: async (m, o) => { const ack = { ...await p.destination.receiver.receive(m, o) }; mutate(ack); return ack; } }).run(), phase('receipt'));
    assert.equal((await p.outbox.pending()).length, 1);
    assert.equal((await p.pump().run()).replays, 1);
  });
}

test('sender must explicitly accept the receivers retained omission decision', async t => {
  const p = await pair(t, { receiverOptions: { onConflict: () => 'omit' } }); await recordOne(p);
  p.destination.target.db.exec("INSERT INTO items VALUES(1,'local')");
  await assert.rejects(p.pump().run(), phase('receipt'));
  assert.equal((await p.outbox.pending()).length, 1);
  const result = await p.pump({ allowOmissions: true }).run();
  assert.equal(result.omitted, 1); assert.equal(result.replays, 1);
  assert.deepEqual(rows(p.destination.target), [{ id: 1, value: 'local' }]);
});

test('run and byte limits leave the next ordered message pending', async t => {
  const p = await pair(t); const first = await recordOne(p);
  await p.record('INSERT INTO items VALUES(?,?)', [2n, 'two'], 'source:2');
  let reads = 0;
  const pump = new ChangesetDeliveryPump(adapt(p.outbox, { read: async id => { reads++; return p.outbox.read(id); } }), p.pumpOptions);
  const one = await pump.run({ maxBytes: first.delivery.byteLength });
  assert.equal(one.stopped, 'limit'); assert.equal(one.deliveries, 1); assert.equal(reads, 1);
  assert.equal((await p.outbox.pending())[0].deliveryId, 'source:2');
  const two = await pump.run({ maxDeliveries: 1 });
  assert.equal(two.deliveries, 1); assert.equal(two.stopped, 'limit');
  assert.equal((await pump.run()).stopped, 'empty');
});

for (const mode of ['message', 'run']) {
  test(`${mode} byte admission refuses an oversized head before payload load`, async t => {
    const p = await pair(t); await recordOne(p);
    let reads = 0;
    const pump = new ChangesetDeliveryPump(adapt(p.outbox, { read: async () => { reads++; assert.fail('oversized load'); } }),
      { ...p.pumpOptions, ...(mode === 'message' ? { maxMessageBytes: 1 } : {}) });
    await assert.rejects(pump.run(mode === 'run' ? { maxBytes: 1 } : {}), error => error.code === 'ERR_FSQLITE_DELIVERY_LIMIT');
    assert.equal(reads, 0); assert.equal((await p.outbox.pending()).length, 1);
  });
}

test('corrupt source bytes never reach transport', async t => {
  const p = await pair(t); await recordOne(p);
  p.source.db.exec("UPDATE __fsqlite_changeset_outbox SET payload=zeroblob(byte_length)");
  await assert.rejects(p.pump({ deliver: async () => assert.fail('corrupt send') }).run(), phase('source-read'));
  assert.equal((await p.outbox.pending()).length, 1);
});

test('acknowledgement racing selection and payload read is confirmed, not sent twice', async t => {
  const p = await pair(t); await recordOne(p); let raced = false, confirmations = 0;
  const wrapped = adapt(p.outbox, { pending: async o => {
    const selected = await p.outbox.pending(o);
    if (selected.length && !raced) {
      raced = true; const loaded = await p.outbox.read(selected[0].deliveryId);
      await p.destination.receiver.receive(envelope(loaded.changeset));
      await p.outbox.acknowledge(selected[0].deliveryId, selected[0].sha256);
    }
    return selected;
  } });
  const result = await new ChangesetDeliveryPump(wrapped, { ...p.pumpOptions,
    confirmSource: async () => { confirmations++; }, deliver: async () => assert.fail('already confirmed by peer') }).run();
  assert.equal(result.deliveries, 0); assert.equal(result.alreadyAcknowledged, 1); assert.equal(confirmations, 2);
});

test('missing or changed selected metadata stops instead of skipping work', async t => {
  const p = await pair(t); await recordOne(p);
  for (const read of [async () => null, async id => {
    const loaded = await p.outbox.read(id); return { ...loaded, delivery: { ...loaded.delivery, sequence: 999n } };
  }]) {
    await assert.rejects(new ChangesetDeliveryPump(adapt(p.outbox, { read }), p.pumpOptions).run(), phase('source-read'));
    assert.equal((await p.outbox.pending()).length, 1);
  }
});

test('empty changesets still receive confirmed identity receipts', async t => {
  const p = await pair(t);
  await p.outbox.record(async () => {}, { tables: ['items'], deliveryId: 'source:empty' });
  const result = await p.pump().run();
  assert.equal(result.deliveries, 1); assert.equal(result.bytes, 0); assert.equal(result.applied, 0);
  assert.equal((await p.outbox.read('source:empty')).delivery.acknowledged, true);
});

test('transport cancellation waits for settlement and cannot acknowledge a late response', async t => {
  const p = await pair(t); await recordOne(p);
  const entered = gate(), finish = gate(), controller = new AbortController(); let settled = false;
  const pump = p.pump({ deliver: async m => { entered.release(); await finish.promise;
    return p.destination.receiver.receive(m); // Model a remote operation that cannot be cancelled.
  } });
  const run = pump.run({ signal: controller.signal });
  const checked = assert.rejects(run, error => error.code === 'ERR_FSQLITE_DELIVERY_CANCELLED');
  run.then(() => { settled = true; }, () => { settled = true; });
  await entered.promise; controller.abort('stop'); await pause(10); assert.equal(settled, false);
  await assert.rejects(pump.run(), error => error.code === 'ERR_FSQLITE_DELIVERY_BUSY');
  finish.release(); await checked;
  assert.equal((await p.outbox.pending()).length, 1); assert.equal((await p.pump().run()).replays, 1);
});

test('cancellation during a successful source ACK still drains source confirmation', async t => {
  const p = await pair(t); await recordOne(p); const controller = new AbortController(); let confirmations = 0;
  const wrapped = adapt(p.outbox, { acknowledge: async (id, sha) => {
    const result = await p.outbox.acknowledge(id, sha); controller.abort('after ack'); return result;
  } });
  await assert.rejects(new ChangesetDeliveryPump(wrapped, { ...p.pumpOptions,
    confirmSource: async () => { confirmations++; } }).run({ signal: controller.signal }), error => error.code === 'ERR_FSQLITE_DELIVERY_CANCELLED');
  assert.equal(confirmations, 3); assert.equal((await p.outbox.pending()).length, 0);
});

test('delivery timeout passes only the remaining budget to transport and drains it', async t => {
  const p = await pair(t); await recordOne(p); const entered = gate(), finish = gate(); let settled = false, seenTimeout;
  const pump = p.pump({ confirmSource: async () => { await pause(5); }, deliver: async (m, o) => {
    seenTimeout = o.timeoutMs; entered.release(); await finish.promise; return p.destination.receiver.receive(m);
  } });
  const run = pump.run({ timeoutMs: 200 });
  const checked = assert.rejects(run, error => error.code === 'ERR_FSQLITE_DELIVERY_TIMEOUT');
  run.then(() => { settled = true; }, () => { settled = true; });
  await entered.promise; assert.ok(seenTimeout > 0 && seenTimeout < 200);
  await pause(210); assert.equal(settled, false); finish.release(); await checked;
  assert.equal((await p.outbox.pending()).length, 1);
});

test('new source writes between confirmations are included before sending', async t => {
  const p = await pair(t); let confirms = 0, recorded = false;
  const wrapped = adapt(p.outbox, { pending: async o => {
    if (!recorded) { recorded = true; await recordOne(p); }
    return p.outbox.pending(o);
  } });
  const result = await new ChangesetDeliveryPump(wrapped, { ...p.pumpOptions,
    confirmSource: async () => { confirms++; }, deliver: async (m, o) => {
      assert.equal(confirms, 2); return p.destination.receiver.receive(m, o);
    } }).run();
  assert.equal(result.deliveries, 1); assert.equal(confirms, 3);
});

for (const cut of ['receiver-committed', 'source-acknowledged']) {
  test(`SIGKILL at ${cut} recovers delivery from independently reopened files`, async t => {
    const directory = mkdtempSync(join(tmpdir(), 'fsqlite-delivery-cut-'));
    const sourcePath = join(directory, 'source.db'), receiverPath = join(directory, 'receiver.db');
    const p = await pair(t, { sourcePath, receiverPath }); await recordOne(p);
    const module = new URL('../src/changeset-delivery.ts', import.meta.url).href;
    const outboxModule = new URL('../src/changeset-outbox.ts', import.meta.url).href;
    const loader = new URL('./helpers/source-loader.mjs', import.meta.url).href;
    const child = spawnSync(process.execPath, ['--experimental-loader=' + loader, '--input-type=module', '-e', `
      import { DatabaseSync } from 'node:sqlite';
      import { ChangesetReceiver, ChangesetDeliveryPump } from ${JSON.stringify(module)};
      import { ChangesetOutbox } from ${JSON.stringify(outboxModule)};
      ${SqlTarget.toString()}
      const source = new SqlTarget(process.argv[1]), destination = new SqlTarget(process.argv[2]);
      const receiver = new ChangesetReceiver(destination, { receiverId: 'replica:A', tables: ['items'], confirmCommit: async () => {} });
      let confirmations = 0;
      const kill = () => { console.log('CUT:' + process.argv[3]); process.kill(process.pid, 'SIGKILL'); };
      const pump = new ChangesetDeliveryPump(new ChangesetOutbox(source), {
        receiverId: 'replica:A', confirmSource: async () => {
          if (++confirmations === 3 && process.argv[3] === 'source-acknowledged') kill();
        }, deliver: async (message, options) => {
          const receipt = await receiver.receive(message, options);
          if (process.argv[3] === 'receiver-committed') kill();
          return receipt;
        },
      });
      await pump.run(); process.exitCode = 20;
      source.db.close(); destination.db.close();`, sourcePath, receiverPath, cut], { encoding: 'utf8', timeout: 15_000 });
    assert.equal(child.signal, 'SIGKILL', child.stderr); assert.match(child.stdout, new RegExp('CUT:' + cut));
    const recovered = await pair(t, { sourcePath, receiverPath });
    assert.deepEqual(rows(recovered.destination.target), [{ id: 1, value: 'one' }]);
    const pending = await recovered.outbox.pending();
    assert.equal(pending.length, cut === 'receiver-committed' ? 1 : 0);
    const result = await recovered.pump().run();
    assert.equal(result.replays, cut === 'receiver-committed' ? 1 : 0);
    assert.equal((await recovered.outbox.read('source:1')).delivery.acknowledged, true);
    assert.equal((await recovered.outbox.record(async () => assert.fail('source operation replayed'),
      { tables: ['items'], deliveryId: 'source:1' })).replayed, true);
    assert.equal(recovered.destination.target.db.prepare('SELECT count(*) n FROM __fsqlite_changeset_receipts').get().n, 1);
  });
}

test('real SQLite image barriers retain source bytes until a receiver snapshot is confirmed', async t => {
  const directory = mkdtempSync(join(tmpdir(), 'fsqlite-delivery-images-'));
  const sourceImages = [], receiverImages = [];
  let receiverTarget;
  const save = (target, files, kind) => {
    const path = join(directory, `${kind}-${files.length}.db`);
    target.db.exec(`VACUUM INTO '${path.replaceAll("'", "''")}'`); files.push(path);
  };
  const p = await pair(t, { receiverOptions: { confirmCommit: async () => {
    save(receiverTarget, receiverImages, 'receiver');
    if (receiverImages.length === 1) throw new Error('publication completed but response was lost');
  } } });
  receiverTarget = p.destination.target; await recordOne(p);
  const pump = p.pump({ confirmSource: async () => { save(p.source, sourceImages, 'source'); } });
  await assert.rejects(pump.run(), phase('transport'));
  const readImage = (path, sql) => { const db = new DatabaseSync(path); try { return { ...db.prepare(sql).get() }; } finally { db.close(); } };
  assert.deepEqual(readImage(sourceImages.at(-1), 'SELECT acknowledged, length(payload) > 0 retained FROM __fsqlite_changeset_outbox'), { acknowledged: 0, retained: 1 });
  assert.deepEqual(readImage(receiverImages[0], 'SELECT value FROM items'), { value: 'one' });
  assert.deepEqual(readImage(receiverImages[0], 'SELECT applied FROM __fsqlite_changeset_receipts'), { applied: 1 });
  assert.equal((await pump.run()).replays, 1);
  assert.equal(receiverImages.length, 2);
  assert.deepEqual(readImage(sourceImages.at(-1), 'SELECT acknowledged, length(payload) retained FROM __fsqlite_changeset_outbox'), { acknowledged: 1, retained: 0 });
  assert.deepEqual(readImage(receiverImages.at(-1), 'SELECT value FROM items'), { value: 'one' });
});

test('two file-backed senders overlap, then recover without duplicating receiver SQL', async t => {
  const directory = mkdtempSync(join(tmpdir(), 'fsqlite-delivery-overlap-'));
  const options = { sourcePath: join(directory, 'source.db'), receiverPath: join(directory, 'receiver.db') };
  const first = await pair(t, options); await recordOne(first);
  const second = await pair(t, options);
  const both = gate(); let arrivals = 0;
  const deliver = p => async (message, controls) => {
    if (++arrivals === 2) both.release(); await both.promise;
    return p.destination.receiver.receive(message, controls);
  };
  const attempts = await Promise.allSettled([first.pump({ deliver: deliver(first) }).run(), second.pump({ deliver: deliver(second) }).run()]);
  assert.equal(arrivals, 2); assert.ok(attempts.every(r => r.status === 'fulfilled' || r.reason.phase === 'transport' || r.reason.phase === 'source-ack'));
  await first.pump().run();
  assert.deepEqual(rows(first.destination.target), [{ id: 1, value: 'one' }]);
  assert.equal(first.destination.target.db.prepare('SELECT count(*) n FROM __fsqlite_changeset_receipts').get().n, 1);
  assert.equal((await first.outbox.pending()).length, 0);
});

test('invalid run admission never confirms, reads or submits and releases the instance', async t => {
  const p = await pair(t); let confirmations = 0;
  const pump = p.pump({ confirmSource: async () => { confirmations++; } });
  for (const bad of [{ maxDeliveries: 0 }, { maxDeliveries: 10_001 }, { maxDeliveries: 1.5 },
    { maxBytes: 0 }, { maxBytes: NaN }, { timeoutMs: 0 }, { timeoutMs: -1 }, { signal: {} }]) {
    await assert.rejects(pump.run(bad)); assert.equal(confirmations, 0);
  }
  assert.equal((await pump.run()).stopped, 'empty'); assert.equal(confirmations, 1);
});

test('a claimed successful ACK that leaves the same pending head cannot spin or skip it', async t => {
  const p = await pair(t); await recordOne(p); let sends = 0;
  const pump = new ChangesetDeliveryPump(adapt(p.outbox, { acknowledge: async () => true }), {
    ...p.pumpOptions, deliver: async (m, o) => { sends++; return p.destination.receiver.receive(m, o); },
  });
  await assert.rejects(pump.run(), phase('source-read')); assert.equal(sends, 1);
  assert.equal((await p.pump().run()).replays, 1);
});
