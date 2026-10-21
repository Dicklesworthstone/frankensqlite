// Production pump, ordered sender, receiver, ledger and SQL application.
// The source is an explicitly small persistent SQL port, NOT ChangesetOutbox.
// No SQL results are synthesized; injected hooks delay or fail real operations.
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { createHash } from 'node:crypto';
import { mkdtempSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { ChangesetDeliveryPump } from '../src/changeset-delivery.ts';
import { ChangesetOrder } from '../src/changeset-order.ts';
import { createOrderedChangesetReceiver, createOrderedChangesetTransport,
  createOrderedChangesetBatchTransport } from '../src/changeset-ordered-delivery.ts';
import { applyChangeset } from '../src/changeset-apply.ts';
import { captureChangeset } from '../src/changeset-capture.ts';
import { FrankenSQLiteError } from '../src/errors.ts';

const hash = bytes => createHash('sha256').update(bytes).digest('hex');
const schema = 'CREATE TABLE IF NOT EXISTS items(id INTEGER PRIMARY KEY,value TEXT UNIQUE)';
const gate = () => { let resolve; const promise = new Promise(r => { resolve = r; }); return { promise, resolve }; };
const tick = () => new Promise(r => setImmediate(r));
const code = suffix => error => error?.code === `ERR_FSQLITE_DELIVERY_${suffix}`;
const rows = (db, sql = 'SELECT * FROM items ORDER BY id', params = []) => {
  const stmt = db.prepare(sql); stmt.setReadBigInts(true); stmt.setReturnArrays(true);
  return stmt.all(...params);
};
class Owner {
  constructor(path = ':memory:', ddl = schema) {
    this.db = new DatabaseSync(path); this.db.exec('PRAGMA foreign_keys=ON;PRAGMA recursive_triggers=ON;' + ddl);
    this.depth = 0; this.serial = 0; this.beforeSql = null; this.afterCommit = null;
  }
  async transaction(work) {
    const depth = this.depth++, name = `batch_pump_${++this.serial}`;
    this.db.exec(depth ? `SAVEPOINT ${name}` : 'BEGIN');
    let open = true, committed = false;
    const check = () => assert.ok(open, 'expired owner');
    const tx = {
      execute: async (sql, params = []) => {
        check(); await this.beforeSql?.('execute', sql);
        try { return Number(this.db.prepare(sql).run(...params).changes); }
        catch (e) {
          if ((e.errcode & 255) === 19) throw new FrankenSQLiteError({ code: 'SQLITE_CONSTRAINT', sqliteCode: 19,
            extendedCode: e.errcode, message: e.message });
          throw e;
        }
      },
      query: async (sql, params = []) => { check(); await this.beforeSql?.('query', sql); return { rowArrays: rows(this.db, sql, params) }; },
      transaction: work => { check(); return this.transaction(work); },
    };
    try {
      const value = await work(tx); open = false;
      this.db.exec(depth ? `RELEASE ${name}` : 'COMMIT'); committed = true;
      if (!depth) await this.afterCommit?.();
      return value;
    } catch (e) {
      open = false; if (!committed) this.db.exec(depth ? `ROLLBACK TO ${name}; RELEASE ${name}` : 'ROLLBACK');
      throw e;
    } finally { open = false; this.depth--; }
  }
  close() { this.db.close(); }
}
class Source extends Owner {
  constructor(path = ':memory:') {
    super(path);
    this.db.exec('CREATE TABLE IF NOT EXISTS outgoing(seq INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT UNIQUE NOT NULL,sha TEXT NOT NULL,size INTEGER NOT NULL,n INTEGER NOT NULL,acked INTEGER NOT NULL,wire BLOB NOT NULL)');
    this.reads = []; this.acks = []; this.pages = []; this.confirms = 0;
    this.beforeRead = null; this.afterRead = null; this.beforeAck = null; this.afterAck = null;
    this.onConfirm = null; this.onPending = null;
  }
  async record(id, value = `value-${id}`) {
    return this.transaction(async tx => {
      const captured = await captureChangeset({ transaction: work => work(tx) },
        work => work.execute('INSERT INTO items VALUES(?,?)', [BigInt(id), value]), { tables: ['items'] });
      const wire = captured.changeset, identity = `source:${id}`;
      await tx.execute('INSERT INTO outgoing(id,sha,size,n,acked,wire) VALUES(?,?,?,?,0,?)',
        [identity, hash(wire), BigInt(wire.length), BigInt(captured.changes), wire]);
      return identity;
    });
  }
  metadata(row) {
    return { sequence: row[0], deliveryId: row[1], sha256: row[2], byteLength: Number(row[3]),
      changes: Number(row[4]), acknowledged: row[5] === 1n };
  }
  async pending({ limit = 100 } = {}) {
    this.pages.push(limit);
    const page = await this.transaction(async () => rows(this.db,
      'SELECT seq,id,sha,size,n,acked FROM outgoing WHERE acked=0 ORDER BY seq LIMIT ?', [BigInt(limit)]).map(r => this.metadata(r)));
    return this.onPending ? this.onPending(page, limit) : page;
  }
  async read(id) {
    this.reads.push(id); await this.beforeRead?.(id);
    const value = await this.transaction(async () => {
      const found = rows(this.db, 'SELECT seq,id,sha,size,n,acked,wire FROM outgoing WHERE id=?', [id]);
      if (!found.length) return null;
      return { delivery: this.metadata(found[0]), changeset: found[0][5] === 1n ? null : new Uint8Array(found[0][6]) };
    });
    return this.afterRead ? this.afterRead(id, value) : value;
  }
  async acknowledge(id, sha) {
    this.acks.push(id); await this.beforeAck?.(id);
    const changed = await this.transaction(async tx => {
      const found = rows(this.db, 'SELECT sha,acked FROM outgoing WHERE id=?', [id]);
      assert.equal(found[0]?.[0], sha, 'wrong ACK identity');
      if (found[0][1] === 1n) return false;
      return (await tx.execute('UPDATE outgoing SET acked=1,wire=zeroblob(0) WHERE id=? AND sha=?', [id, sha])) === 1;
    });
    await this.afterAck?.(id); return changed;
  }
  async confirm() { assert.equal(this.depth, 0, 'source confirmation inside SQL'); this.confirms++; await this.onConfirm?.(); }
  pendingIds() { return rows(this.db, 'SELECT id FROM outgoing WHERE acked=0 ORDER BY seq').flat(); }
}
async function setup(t, n = 3, config = {}) {
  const source = new Source(config.sourcePath), destination = new Owner(config.receiverPath);
  t.after(() => { source.close(); destination.close(); });
  for (let i = 1; i <= n; i++) await source.record(i);
  const order = new ChangesetOrder(destination, { receiverId: 'east', sourceId: 'source:epoch' });
  await order.initialize();
  const f = { source, destination, order, sent: [], confirmed: 0, calls: [], appOptions: {},
    beforeDelivery: null, afterDelivery: null, onConfirm: null };
  f.receiver = await createOrderedChangesetReceiver(order, {
    apply: (target, message, controls) => {
      f.calls.push(message.deliveryId);
      return applyChangeset(target, message.changeset, { tables: ['items'], deliveryId: message.deliveryId,
        ...controls, ...f.appOptions });
    },
    confirmCommit: async () => { assert.equal(destination.depth, 0); f.confirmed++; await f.onConfirm?.(); },
  });
  const wireOptions = { receiverId: 'east', streamId: 'source:epoch',
    deliver: async (messages, controls) => {
      assert.equal(source.depth, 0, 'transport held a source transaction');
      f.sent.push(messages.map(m => m.deliveryId));
      await f.beforeDelivery?.(messages, controls);
      const response = await f.receiver.receiveBatch(messages, controls);
      return f.afterDelivery ? f.afterDelivery(response, messages) : response;
    } };
  f.batch = createOrderedChangesetBatchTransport(source, wireOptions);
  f.single = createOrderedChangesetTransport(source, { ...wireOptions, deliver: (msg, controls) => f.receiver.receive(msg, controls) });
  f.makePump = (options = {}) => new ChangesetDeliveryPump(source, {
    receiverId: 'east', deliver: f.single, deliverBatch: f.batch,
    confirmSource: () => source.confirm(), ...options,
  });
  f.pump = f.makePump();
  return f;
}

// Entire publication path uses the production capture, pump, sender, receiver,
// ledger, SQL applier and confirmation ownership around the SQL source port.
test('batch pump drains captured source rows in bounded contiguous confirmed windows', async t => {
  const f = await setup(t, 5); const size = Number(rows(f.source.db, 'SELECT sum(size) FROM outgoing')[0][0]);
  const result = await f.pump.runBatch({ maxBatchDeliveries: 2 });
  assert.deepEqual(result, { deliveries: 5, bytes: size, applied: 5, omitted: 0, replays: 0, alreadyAcknowledged: 0, stopped: 'empty' });
  assert.ok(Object.isFrozen(result)); assert.deepEqual(f.sent, [['source:1','source:2'],['source:3','source:4'],['source:5']]);
  assert.equal(f.confirmed, 3); assert.equal(f.source.confirms, 1 + 3 + 5);
  assert.deepEqual(rows(f.destination.db), rows(f.source.db));
  assert.deepEqual(f.source.pendingIds(), []);
  assert.deepEqual(rows(f.source.db, 'SELECT seq,length(wire) FROM outgoing'), [[1n,0n],[2n,0n],[3n,0n],[4n,0n],[5n,0n]]);
  assert.equal((await f.order.head()).sequence, 5n);
});

test('batch pump validates every receipt before acknowledging any member', async t => {
  for (const bad of ['last-digest','short','swapped','accessor','last-count','last-confirmation']) {
    const f = await setup(t, 3); let accesses = 0;
    // Test the pump independently of the stronger ordered sender's own checks.
    f.pump = f.makePump({ deliverBatch: async (messages, controls) => {
      const valid = await f.batch(messages, controls); const response = valid.map(v => ({ ...v }));
      if (bad === 'last-digest') response[2].sha256 = '0'.repeat(64);
      if (bad === 'short') response.pop();
      if (bad === 'swapped') response.reverse();
      if (bad === 'accessor') Object.defineProperty(response, '2', { get() { accesses++; return valid[2]; } });
      if (bad === 'last-count') response[2].applied = 0;
      if (bad === 'last-confirmation') response[2].confirmed = false;
      return response;
    } });
    await assert.rejects(f.pump.runBatch(), code('RECEIPT'));
    assert.equal(accesses, 0); assert.equal(f.source.acks.length, 0);
    assert.equal(f.source.pendingIds().length, 3); assert.equal(rows(f.destination.db).length, 3);
    assert.equal((await f.makePump().runBatch()).replays, 3);
    assert.equal(f.calls.length, 3);
  }
});

test('a late omitted row requires consent for the entire batch before any reclamation', async t => {
  const f = await setup(t, 3);
  f.destination.db.exec("INSERT INTO items VALUES(9,'value-3')");
  f.appOptions.constraintConflicts = 'isolate'; f.appOptions.onConflict = c => { assert.equal(c.kind, 'constraint'); return 'omit'; };
  await assert.rejects(f.pump.runBatch(), code('RECEIPT'));
  assert.equal(f.source.acks.length, 0); assert.equal(f.source.pendingIds().length, 3);
  assert.equal((await f.order.head()).sequence, 3n);
  const result = await f.makePump({ allowOmissions: true }).runBatch();
  assert.equal(result.omitted, 1); assert.equal(result.applied, 2); assert.equal(result.replays, 3);
  assert.equal(f.calls.length, 3); assert.deepEqual(f.source.pendingIds(), []);
});

test('lost remote response retains every source payload and exact retry runs no application', async t => {
  const f = await setup(t); f.afterDelivery = () => { throw new Error('lost batch response'); };
  await assert.rejects(f.pump.runBatch(), e => e.phase === 'transport');
  assert.equal(f.source.acks.length, 0); assert.equal(f.calls.length, 3); assert.equal(f.source.pendingIds().length, 3);
  f.afterDelivery = null;
  const result = await f.pump.runBatch();
  assert.equal(result.replays, 3); assert.equal(f.calls.length, 3); assert.equal(f.confirmed, 2);
});

test('failed source ACK leaves a confirmed prefix and replays only the pending suffix', async t => {
  const f = await setup(t);
  f.source.beforeAck = id => { if (id === 'source:2') throw new Error('failed ACK'); };
  await assert.rejects(f.pump.runBatch(), e => e.phase === 'source-ack' && e.deliveryId === 'source:2');
  assert.deepEqual(f.source.pendingIds(), ['source:2','source:3']);
  assert.equal(f.source.confirms, 3); // start, admitted source window, confirmed first ACK
  f.source.beforeAck = null;
  const result = await f.pump.runBatch();
  assert.equal(result.deliveries, 2); assert.equal(result.replays, 2);
  assert.deepEqual(f.sent, [['source:1','source:2','source:3'],['source:2','source:3']]);
  assert.equal(f.calls.length, 3);
});

test('source confirmation failure after ACK is reconciled before selecting a fresh suffix', async t => {
  const f = await setup(t); let reconcile = false;
  f.source.onConfirm = () => { if (f.source.confirms === 3) throw new Error('source checkpoint lost'); if (f.source.confirms === 4) reconcile = true; };
  await assert.rejects(f.pump.runBatch(), e => e.phase === 'source-confirm' && e.deliveryId === 'source:1');
  assert.deepEqual(f.source.pendingIds(), ['source:2','source:3']);
  f.source.beforeRead = () => assert.ok(reconcile, 'read preceded source recovery');
  assert.equal((await f.pump.runBatch()).replays, 2);
});

for (const ending of ['cancel','timeout']) test(`batch pump ${ending} joins remote confirmation without ACK or a second run`, async t => {
  const f = await setup(t), entered = gate(), release = gate(), abort = new AbortController();
  f.onConfirm = async () => { entered.resolve(); await release.promise; };
  const operation = f.pump.runBatch(ending === 'cancel' ? { signal: abort.signal } : { timeoutMs: 250 });
  let done = false; void operation.then(() => { done = true; }, () => { done = true; });
  try {
    await Promise.race([entered.promise, operation.then(() => assert.fail('no held confirmation'))]);
    if (ending === 'cancel') abort.abort(); else await new Promise(r => setTimeout(r, 280));
    await tick(); assert.equal(done, false); assert.equal(f.source.acks.length, 0);
    await assert.rejects(f.pump.run(), code('BUSY')); await assert.rejects(f.pump.runBatch(), code('BUSY'));
    release.resolve(); await assert.rejects(operation, error => error.phase === 'transport' &&
      // DeliveryBudget's timeout also aborts the signal forwarded to transport.
      error.cause?.code === 'ERR_FSQLITE_ORDERED_CANCELLED');
    assert.equal(f.source.pendingIds().length, 3); f.onConfirm = null;
    assert.equal((await f.pump.runBatch()).replays, 3);
  } finally { release.resolve(); await Promise.allSettled([operation]); }
});

test('cancellation during an admitted source ACK still joins its confirmation then stops the suffix', async t => {
  const f = await setup(t), entered = gate(), release = gate(), abort = new AbortController();
  f.source.afterAck = async id => { if (id === 'source:1') { entered.resolve(); await release.promise; } };
  const operation = f.pump.runBatch({ signal: abort.signal }); let done = false;
  void operation.then(() => { done = true; }, () => { done = true; });
  try {
    await Promise.race([entered.promise, operation]); abort.abort(); await tick(); assert.equal(done, false);
    release.resolve(); await assert.rejects(operation, code('CANCELLED'));
    assert.equal(f.source.confirms, 3); assert.deepEqual(f.source.acks, ['source:1']);
    assert.deepEqual(f.source.pendingIds(), ['source:2','source:3']);
    f.source.afterAck = null; assert.equal((await f.pump.runBatch()).replays, 2);
  } finally { release.resolve(); await Promise.allSettled([operation]); }
});

test('cancellation during an admitted source read waits without sending or acknowledging', async t => {
  const f = await setup(t), entered = gate(), release = gate(), abort = new AbortController();
  f.source.beforeRead = async () => { entered.resolve(); await release.promise; };
  const operation = f.pump.runBatch({ signal: abort.signal }); let done = false;
  void operation.then(() => { done = true; }, () => { done = true; });
  try {
    await Promise.race([entered.promise, operation]); abort.abort(); await tick(); assert.equal(done, false);
    await assert.rejects(f.pump.run(), code('BUSY')); release.resolve(); await assert.rejects(operation, code('CANCELLED'));
    assert.equal(f.sent.length, 0); assert.equal(f.source.acks.length, 0); assert.equal(f.source.pendingIds().length, 3);
  } finally { release.resolve(); await Promise.allSettled([operation]); }
});

test('confirmed empty source and pre-cancellation preserve existing recovery semantics', async t => {
  const f = await setup(t, 0), c = new AbortController(); c.abort();
  await assert.rejects(f.pump.runBatch({ signal: c.signal }), code('CANCELLED'));
  assert.equal(f.source.confirms, 0); assert.equal(f.source.pages.length, 0);
  const r = await f.pump.runBatch(); assert.equal(r.stopped, 'empty'); assert.equal(r.deliveries, 0);
  assert.equal(f.source.confirms, 1);
  f.source.onConfirm = () => { throw new Error('cannot confirm empty source'); };
  await assert.rejects(f.pump.runBatch(), e => e.phase === 'source-confirm');
});

test('explicit batch transport and bounded configuration are required before touching source state', async t => {
  const f = await setup(t, 0);
  await assert.rejects(f.makePump({ deliverBatch: undefined }).runBatch(), code('INPUT'));
  for (const options of [{ maxBatchDeliveries: 0 }, { maxBatchDeliveries: 257 }, { maxBatchBytes: 0 },
    { maxBatchBytes: 64 * 1024 * 1024 + 1 }, { maxDeliveries: 0 }, { maxBytes: 0 }])
    await assert.rejects(f.pump.runBatch(options), code('INPUT'));
  assert.equal(f.source.confirms, 0); assert.equal(f.source.pages.length, 0);
  assert.throws(() => f.makePump({ deliverBatch: 'not a function' }), code('INPUT'));
});

test('run and exchange byte/delivery budgets are exact and never skip the oldest row', async t => {
  const f = await setup(t, 5); const size = Number(rows(f.source.db, 'SELECT size FROM outgoing LIMIT 1')[0][0]);
  await assert.rejects(f.pump.runBatch({ maxBatchBytes: size - 1 }), code('LIMIT'));
  await assert.rejects(f.pump.runBatch({ maxBytes: size - 1 }), code('LIMIT'));
  assert.equal(f.sent.length, 0); assert.equal(f.source.reads.length, 0);
  const r = await f.pump.runBatch({ maxBatchBytes: 2 * size, maxBytes: 3 * size });
  assert.equal(r.stopped, 'limit'); assert.equal(r.deliveries, 3); assert.equal(r.bytes, 3 * size);
  assert.deepEqual(f.sent, [['source:1','source:2'],['source:3']]);
  assert.deepEqual(f.source.pendingIds(), ['source:4','source:5']);
  const last = await f.pump.runBatch({ maxDeliveries: 1 }); assert.equal(last.deliveries, 1); assert.equal(last.stopped, 'limit');
  assert.deepEqual(f.source.pendingIds(), ['source:5']);
});

test('malformed final source payload prevents the complete window from being transmitted', async t => {
  const f = await setup(t, 3); f.source.db.exec("UPDATE outgoing SET wire=X'54',size=1 WHERE seq=3");
  f.source.db.prepare('UPDATE outgoing SET sha=? WHERE seq=3').run(hash(new Uint8Array([84])));
  await assert.rejects(f.pump.runBatch());
  assert.equal(f.sent.length, 0); assert.equal(f.source.acks.length, 0); assert.equal(f.source.pendingIds().length, 3);
});

test('source metadata is captured before asynchronous reads and dishonest rereads cannot change identity', async t => {
  const f = await setup(t, 3); let page;
  f.source.onPending = p => { page = p; return p; };
  f.source.beforeRead = () => { if (page.length) { page[2].deliveryId = 'changed'; page.length = 0; } };
  await f.pump.runBatch(); assert.equal(f.sent[0].length, 3);
  const bad = await setup(t, 2);
  bad.source.afterRead = (id, result) => id === 'source:2' ? { ...result, delivery: { ...result.delivery, sha256: '0'.repeat(64) } } : result;
  await assert.rejects(bad.pump.runBatch(), code('INPUT')); assert.equal(bad.sent.length, 0); assert.equal(bad.source.acks.length, 0);
});

test('pending-page accessors, duplicate IDs, nonmonotonic sequences and oversized pages are refused', async t => {
  for (const variant of ['getter','id','sequence','oversized']) {
    const f = await setup(t, 2); let invoked = 0;
    f.source.onPending = (p, n) => {
      if (variant === 'getter') Object.defineProperty(p, '1', { get() { invoked++; return p[0]; } });
      if (variant === 'id') p[1].deliveryId = p[0].deliveryId;
      if (variant === 'sequence') p[1].sequence = p[0].sequence;
      if (variant === 'oversized') return Array.from({ length: n + 1 }, () => p[0]);
      return p;
    };
    await assert.rejects(f.pump.runBatch(), code('INPUT')); assert.equal(invoked, 0);
    assert.equal(f.source.reads.length, 0); assert.equal(f.sent.length, 0);
  }
});

test('acknowledged gaps split contiguous windows without renumbering or skipping retained work', async t => {
  const f = await setup(t, 3); const messages = [];
  for (let i = 1; i <= 3; i++) {
    const v = await f.source.read(`source:${i}`);
    messages.push({ protocol: 'fsqlite-changeset-v1', receiverId: 'east', deliveryId: v.delivery.deliveryId,
      sha256: v.delivery.sha256, changeset: v.changeset });
  }
  await f.batch(messages, {});
  const middle = await f.source.read('source:2'); await f.source.acknowledge('source:2', middle.delivery.sha256);
  f.sent.length = 0; f.source.acks.length = 0;
  const result = await f.pump.runBatch(); assert.equal(result.deliveries, 2); assert.equal(result.replays, 2);
  assert.deepEqual(f.sent, [['source:1'],['source:3']]); assert.deepEqual(f.source.acks, ['source:1','source:3']);
});

for (const raced of ['source:1','source:2']) test(`ACK race on ${raced} confirms before continuing the oldest pending prefix`, async t => {
  const f = await setup(t, 3); const preload = [];
  for (let i = 1; i <= 3; i++) {
    const v = await f.source.read(`source:${i}`); preload.push({ protocol: 'fsqlite-changeset-v1', receiverId: 'east',
      deliveryId: v.delivery.deliveryId, sha256: v.delivery.sha256, changeset: v.changeset });
  }
  await f.batch(preload, {}); f.sent.length = 0; let race = true;
  f.source.beforeRead = async id => {
    if (id === raced && race) { race = false; const saved = rows(f.source.db, 'SELECT sha FROM outgoing WHERE id=?', [id])[0][0];
      await f.source.acknowledge(id, saved); }
  };
  const result = await f.pump.runBatch(); assert.equal(result.alreadyAcknowledged, 1);
  assert.equal(result.deliveries, 2); assert.equal(result.replays, 2);
  assert.deepEqual(f.source.pendingIds(), []);
  assert.deepEqual(f.sent, raced === 'source:1' ? [['source:2','source:3']] : [['source:1'],['source:3']]);
});

test('transferred transport buffers do not change acknowledgement lengths or run accounting', async t => {
  const f = await setup(t, 2); const size = Number(rows(f.source.db, 'SELECT sum(size) FROM outgoing')[0][0]);
  const pump = f.makePump({ deliverBatch: async (messages, controls) => {
    const replies = await f.batch(messages, controls);
    for (const message of messages) structuredClone(message.changeset.buffer, { transfer: [message.changeset.buffer] });
    return replies;
  } });
  assert.equal((await pump.runBatch()).bytes, size); assert.deepEqual(f.source.pendingIds(), []);
});

test('batch and single runs share admission while source confirmation is suspended', async t => {
  const f = await setup(t, 1), entered = gate(), release = gate();
  f.source.onConfirm = async () => { entered.resolve(); await release.promise; };
  const operation = f.pump.run();
  try { await entered.promise; await assert.rejects(f.pump.runBatch(), code('BUSY'));
    release.resolve(); await operation; f.source.onConfirm = null;
    assert.equal((await f.pump.runBatch()).stopped, 'empty');
  } finally { release.resolve(); await Promise.allSettled([operation]); }
});

test('constructor captures batch exchange and omission policy rather than retaining mutable options', async t => {
  const f = await setup(t, 2); const options = { receiverId: 'east', deliver: f.single, deliverBatch: f.batch,
    confirmSource: () => f.source.confirm(), allowOmissions: false };
  const pump = new ChangesetDeliveryPump(f.source, options);
  options.deliverBatch = options.confirmSource = () => assert.fail('mutable configuration'); options.allowOmissions = true;
  assert.equal((await pump.runBatch()).deliveries, 2);
});

test('WAL reopen after lost source ACK response resumes suffix without repeating remote work', async t => {
  const dir = mkdtempSync(join(tmpdir(), 'fsqlite-batch-pump-'));
  const f = await setup(t, 3, { sourcePath: join(dir,'source.db'), receiverPath: join(dir,'receiver.db') });
  f.source.db.exec('PRAGMA journal_mode=WAL'); f.destination.db.exec('PRAGMA journal_mode=WAL');
  f.source.afterAck = id => { if (id === 'source:1') throw new Error('lost source commit response'); };
  await assert.rejects(f.pump.runBatch(), e => e.phase === 'source-ack');
  assert.equal(f.calls.length, 3); assert.deepEqual(f.source.pendingIds(), ['source:2','source:3']);
  // Actual close/reopen, not a power-loss simulation. Preserve on-disk evidence.
  f.source.close(); f.destination.close(); f.source.close = f.destination.close = () => {};
  const reopened = await setup(t, 0, { sourcePath: join(dir,'source.db'), receiverPath: join(dir,'receiver.db') });
  const r = await reopened.pump.runBatch(); assert.equal(r.deliveries, 2); assert.equal(r.replays, 2);
  assert.equal(reopened.calls.length, 0); assert.deepEqual(reopened.source.pendingIds(), []);
  assert.deepEqual(rows(reopened.destination.db), rows(reopened.source.db));
});

test('256 zero-byte messages still consume identity limits and leave the next message pending', async t => {
  const f = await setup(t, 0); const empty = new Uint8Array(), sha = hash(empty);
  f.source.db.exec('BEGIN');
  for (let i = 1; i <= 257; i++) f.source.db.prepare('INSERT INTO outgoing(id,sha,size,n,acked,wire) VALUES(?,?,0,0,0,zeroblob(0))').run(`source:${i}`, sha);
  f.source.db.exec('COMMIT');
  const result = await f.pump.runBatch({ maxBatchDeliveries: 256, maxDeliveries: 256, maxBatchBytes: 1, maxBytes: 1 });
  assert.equal(result.deliveries, 256); assert.equal(result.bytes, 0); assert.equal(result.applied, 0); assert.equal(result.stopped, 'limit');
  assert.deepEqual(f.source.pendingIds(), ['source:257']); assert.equal(f.sent.length, 1); assert.equal(f.sent[0].length, 256);
  assert.equal(f.confirmed, 1); assert.equal((await f.order.head()).sequence, 256n);
  assert.equal((await f.pump.runBatch()).deliveries, 1);
});

test('cancellation after all source acknowledgements still requires empty-run recovery confirmation', async t => {
  const f = await setup(t, 1), abort = new AbortController();
  f.source.onConfirm = () => { if (f.source.confirms === 3) abort.abort(); };
  await assert.rejects(f.pump.runBatch({ signal: abort.signal }), code('CANCELLED'));
  assert.deepEqual(f.source.pendingIds(), []); assert.equal(f.confirmed, 1);
  const replay = await f.pump.runBatch(); assert.equal(replay.stopped, 'empty'); assert.equal(replay.deliveries, 0);
  assert.equal(f.source.confirms, 4); assert.equal(f.calls.length, 1);
});

test('post-read source confirmation failure withholds the entire window before transport', async t => {
  const f = await setup(t, 3);
  f.source.onConfirm = () => { if (f.source.confirms === 2) throw new Error('source snapshot not durable'); };
  await assert.rejects(f.pump.runBatch(), e => e.phase === 'source-confirm');
  assert.deepEqual(f.source.reads, ['source:1','source:2','source:3']);
  assert.equal(f.sent.length, 0); assert.equal(f.source.acks.length, 0); assert.deepEqual(rows(f.destination.db), []);
  assert.equal((await f.pump.runBatch()).deliveries, 3);
});
