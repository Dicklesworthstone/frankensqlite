// Production ordered delivery/application against actual native SQLite.
// No HTTP, transport-only stubs, or FrankenSQLite Rust-engine claim.
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { createHash } from 'node:crypto';
import { mkdtempSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { applyChangeset, CHANGESET_RECEIPTS_TABLE } from '../src/changeset-apply.ts';
import { ChangesetOrder, CHANGESET_ORDER_TABLE } from '../src/changeset-order.ts';
import { createOrderedChangesetReceiver, createOrderedChangesetTransport,
  CHANGESET_ORDER_PROTOCOL } from '../src/changeset-ordered-delivery.ts';

const ids = { receiverId: 'east', sourceId: 'source:count-admission' };
const hash = wire => createHash('sha256').update(wire).digest('hex');
const receiptError = error => error?.code === 'ERR_FSQLITE_ORDERED_RECEIPT';
function envelope(format = 'changeset', count = 2) {
  const db = new DatabaseSync(':memory:');
  db.exec('CREATE TABLE items(id INTEGER PRIMARY KEY,value TEXT)');
  const session = db.createSession();
  try {
    for (let i = 1; i <= count; i++) db.prepare('INSERT INTO items VALUES(?,?)').run(i, `v${i}`);
    const changeset = new Uint8Array(session[format]());
    return { protocol: 'fsqlite-changeset-v1', receiverId: ids.receiverId,
      deliveryId: 'source:operation/1', sha256: hash(changeset), changeset,
      order: { protocol: CHANGESET_ORDER_PROTOCOL, streamId: ids.sourceId, sequence: '1' } };
  } finally { session.close(); db.close(); }
}
function rows(db, sql, params = []) {
  const stmt = db.prepare(sql); stmt.setReadBigInts(true); stmt.setReturnArrays(true);
  return stmt.all(...params);
}
class Owner {
  constructor(path = ':memory:') {
    this.db = new DatabaseSync(path);
    this.db.exec('PRAGMA journal_mode=WAL;CREATE TABLE IF NOT EXISTS items(id INTEGER PRIMARY KEY,value TEXT);' +
      'CREATE TABLE IF NOT EXISTS journal(id TEXT PRIMARY KEY,info BLOB)');
    this.transactions = 0; this.active = false;
  }
  async transaction(work) {
    assert.equal(this.active, false); this.active = true; this.transactions++;
    this.db.exec('BEGIN');
    const tx = {
      execute: async (sql, params = []) => Number(this.db.prepare(sql).run(...params).changes),
      query: async (sql, params = []) => ({ rowArrays: rows(this.db, sql, params) }),
    };
    try { const value = await work(tx); this.db.exec('COMMIT'); return value; }
    catch (error) { this.db.exec('ROLLBACK'); throw error; }
    finally { this.active = false; }
  }
  close() { this.db.close(); }
}
async function setup(t, options = {}) {
  const owner = new Owner(options.path); t.after(() => owner.close());
  const order = new ChangesetOrder(owner, ids); await order.initialize();
  const f = { owner, order, calls: 0, confirms: 0, verdict: result => result, onConflict: undefined,
    confirmHook: undefined };
  // A buggy adapter reports the same wrong counters from its SQL callback AND
  // its outer response. The existing applyOnce agreement check cannot catch it.
  f.apply = (target, msg, controls) => target.transaction(async tx => {
    f.calls++;
    const result = await applyChangeset({ transaction: work => work(tx) }, msg.changeset, {
      tables: ['items'], deliveryId: msg.deliveryId, ...controls,
      ...(f.onConflict === undefined ? {} : { onConflict: f.onConflict }),
      onRebase: (hook, bytes) => bytes.length
        ? hook.execute('INSERT INTO journal VALUES(?,?)', [msg.deliveryId, bytes])
        : hook.execute('INSERT INTO journal VALUES(?,zeroblob(0))', [msg.deliveryId]),
    });
    return f.verdict(result);
  }, controls);
  f.receiver = await createOrderedChangesetReceiver(order, {
    apply: f.apply,
    confirmCommit: async () => {
      assert.equal(owner.active, false); f.confirms++; await f.confirmHook?.();
    },
  });
  return f;
}
function unchanged(f) {
  assert.deepEqual(rows(f.owner.db, 'SELECT * FROM items'), []);
  assert.deepEqual(rows(f.owner.db, 'SELECT * FROM journal'), []);
  assert.deepEqual(rows(f.owner.db, `SELECT seq FROM "${CHANGESET_ORDER_TABLE}"`), [[0n]]);
  if (rows(f.owner.db, 'SELECT name FROM sqlite_schema WHERE name=?', [CHANGESET_RECEIPTS_TABLE]).length)
    assert.deepEqual(rows(f.owner.db, `SELECT * FROM "${CHANGESET_RECEIPTS_TABLE}"`), []);
  assert.equal(f.confirms, 0); assert.equal(f.owner.active, false);
}
for (const decision of [{ applied: 1, omitted: 0 }, { applied: 3, omitted: 0 },
  { applied: 0, omitted: 1 }, { applied: 2, omitted: 1 }]) {
  test(`single receipt rejects inconsistent totals atomically: ${JSON.stringify(decision)}`, async t => {
    const f = await setup(t), msg = envelope();
    f.verdict = result => ({ ...result, ...decision });
    await assert.rejects(f.receiver.receive(msg), receiptError);
    unchanged(f); assert.equal(f.calls, 1);
    // Corrected policy can retry the SAME identity: a bad receipt must not have
    // poisoned either the inbox or independent order head.
    f.verdict = result => result;
    const ack = await f.receiver.receive(msg);
    assert.equal(ack.replayed, false); assert.equal(ack.applied, 2); assert.equal(f.confirms, 1);
    assert.deepEqual(rows(f.owner.db, 'SELECT * FROM items ORDER BY id'), [[1n, 'v1'], [2n, 'v2']]);
    const replay = await f.receiver.receive(msg);
    assert.equal(replay.replayed, true); assert.equal(f.calls, 2); assert.equal(f.confirms, 2);
  });
}

test('ordered sender rejection cannot leave a committed, permanently unacceptable replay', async t => {
  const f = await setup(t), msg = envelope();
  const source = { read: async () => ({ delivery: { sequence: 1n, deliveryId: msg.deliveryId,
    sha256: msg.sha256, byteLength: msg.changeset.length, changes: 2, acknowledged: false },
    changeset: msg.changeset }) };
  const send = createOrderedChangesetTransport(source, { receiverId: ids.receiverId, streamId: ids.sourceId,
    deliver: (message, controls) => f.receiver.receive(message, controls) });
  f.verdict = result => ({ ...result, applied: 0 });
  await assert.rejects(send(msg), receiptError);
  // Original sender already rejected, but original receiver COMMITTED the rows,
  // journal, inbox and zero-count order receipt. The failure must happen earlier.
  unchanged(f);
  f.verdict = result => result;
  assert.equal((await send(msg)).applied, 2); assert.equal(f.calls, 2);
});

test('single receiver validates the complete full format before entering SQL or custom application', async t => {
  const f = await setup(t);
  let called = 0;
  const receiver = await createOrderedChangesetReceiver(f.order, {
    apply: target => target.transaction(async tx => {
      called++; await tx.execute("INSERT INTO items VALUES(99,'ignored wire')");
      return { applied: 1, omitted: 0, replayed: false };
    }),
    confirmCommit: async () => { f.confirms++; },
  });
  const valid = envelope(), compact = envelope('patchset');
  const truncated = { ...valid, changeset: valid.changeset.slice(0, -1) };
  truncated.sha256 = hash(truncated.changeset);
  const before = f.owner.transactions;
  for (const msg of [truncated, compact]) await assert.rejects(receiver.receive(msg));
  assert.equal(called, 0); assert.equal(f.owner.transactions, before); unchanged(f);
});

test('single replay refuses legacy inconsistent counters without confirmation or rewriting history', async t => {
  const f = await setup(t), msg = envelope();
  // The generic order ledger admits application-specific bytes and counters;
  // construct the exact legacy result without weakening storage validation.
  await f.order.apply({ sequence: 1n, deliveryId: msg.deliveryId, sha256: msg.sha256, changeset: msg.changeset },
    target => target.transaction(async tx => {
      await tx.execute("INSERT INTO items VALUES(1,'legacy')");
      return { applied: 0, omitted: 0, replayed: false };
    }));
  const head = await f.order.head();
  await assert.rejects(f.receiver.receive(msg), receiptError);
  assert.equal(f.confirms, 0); assert.equal(f.calls, 0);
  assert.deepEqual(await f.order.head(), head);
  assert.deepEqual(rows(f.owner.db, 'SELECT * FROM items'), [[1n, 'legacy']]);
});

test('valid omissions and empty messages retain original counters on single replay', async t => {
  const f = await setup(t), msg = envelope();
  f.owner.db.exec("INSERT INTO items VALUES(1,'local')"); f.onConflict = () => 'omit';
  const ack = await f.receiver.receive(msg);
  assert.deepEqual([ack.applied, ack.omitted], [1, 1]);
  const replay = await f.receiver.receive(msg);
  assert.deepEqual([replay.applied, replay.omitted, replay.replayed], [1, 1, true]);
  assert.equal(f.calls, 1);
  const empty = envelope('changeset', 0); empty.deliveryId = 'source:empty'; empty.order.sequence = '2';
  assert.deepEqual(await f.receiver.receive(empty), { protocol: empty.protocol, receiverId: ids.receiverId,
    deliveryId: empty.deliveryId, sha256: empty.sha256, byteLength: 0, applied: 0, omitted: 0,
    replayed: false, confirmed: true, order: empty.order });
});

test('empty wire cannot advance the order head with nonzero decisions', async t => {
  const f = await setup(t); f.verdict = result => ({ ...result, applied: 1 });
  await assert.rejects(f.receiver.receive(envelope('changeset', 0)), receiptError); unchanged(f);
});

test('validated counts survive WAL reopen and lost confirmation without repeated journal writes', async t => {
  const path = join(mkdtempSync(join(tmpdir(), 'fsqlite-count-admission-')), 'receiver.db');
  const first = await setup(t, { path }), msg = envelope();
  first.confirmHook = () => { throw new Error('lost confirmation'); };
  await assert.rejects(first.receiver.receive(msg), /lost confirmation/);
  assert.equal(first.calls, 1);
  // Close the original SQLite handle before opening the second; the registered
  // close hook is made idempotent without deleting the retained evidence files.
  first.owner.close(); first.owner.close = () => {};
  const next = await setup(t, { path });
  const receipt = await next.receiver.receive(msg);
  assert.deepEqual([receipt.applied, receipt.omitted, receipt.replayed], [2, 0, true]);
  assert.equal(next.calls, 0); assert.equal(next.confirms, 1);
  assert.equal(rows(next.owner.db, 'SELECT * FROM journal').length, 1);
});

for (const changes of [0, 1, 3]) {
  test(`single sender refuses retained count ${changes} before remote commit`, async t => {
    const f = await setup(t), msg = envelope(); let sent = 0;
    const source = { read: async () => ({ delivery: { sequence: 1n, deliveryId: msg.deliveryId,
      sha256: msg.sha256, byteLength: msg.changeset.length, changes, acknowledged: false },
      changeset: msg.changeset }) };
    const send = createOrderedChangesetTransport(source, { receiverId: ids.receiverId, streamId: ids.sourceId,
      deliver: (message, controls) => { sent++; return f.receiver.receive(message, controls); } });
    await assert.rejects(send(msg), receiptError);
    assert.equal(sent, 0); assert.equal(f.calls, 0); unchanged(f);
  });
}

test('single sender refuses invalid full wire before source reads or transport', async () => {
  const valid = envelope(), compact = envelope('patchset');
  const truncated = { ...valid, changeset: valid.changeset.slice(0, -1) };
  truncated.sha256 = hash(truncated.changeset);
  for (const msg of [truncated, compact]) {
    let reads = 0, sent = 0;
    const source = { read: async () => {
      reads++;
      return { delivery: { sequence: 1n, deliveryId: msg.deliveryId, sha256: msg.sha256,
        byteLength: msg.changeset.length, changes: 2, acknowledged: false }, changeset: msg.changeset };
    } };
    const send = createOrderedChangesetTransport(source, { receiverId: ids.receiverId, streamId: ids.sourceId,
      deliver: async () => { sent++; throw new Error('invalid payload reached transport'); } });
    await assert.rejects(send(msg));
    assert.equal(reads, 0); assert.equal(sent, 0);
  }
});
