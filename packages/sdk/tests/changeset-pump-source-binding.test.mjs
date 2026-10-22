// Production receiver, pump and Session application over reference SQLite.
// The source is an explicit two-replica SQL port, not ChangesetFanout storage.
// These tests isolate the pump's routing/port ownership before reclamation.
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { createHash } from 'node:crypto';
import { ChangesetDeliveryPump, ChangesetReceiver } from '../src/changeset-delivery.ts';

const code = e => e?.code === 'ERR_FSQLITE_DELIVERY_INPUT';
const rows = (db, sql, args = []) => {
  const stmt = db.prepare(sql); stmt.setReadBigInts(true); stmt.setReturnArrays(true);
  return stmt.all(...args);
};
const gate = () => { let resolve; const promise = new Promise(r => { resolve = r; }); return { promise, resolve }; };
const schema = 'CREATE TABLE items(id INTEGER PRIMARY KEY,value TEXT)';
const hash = wire => createHash('sha256').update(wire).digest('hex');
function owner(t) {
  const db = new DatabaseSync(':memory:'); db.exec(schema); t.after(() => db.close());
  let depth = 0;
  const target = { async transaction(work) {
    assert.equal(depth, 0); depth++; db.exec('BEGIN');
    try {
      const result = await work({
        execute: async (sql, args = []) => Number(db.prepare(sql).run(...args).changes),
        query: async (sql, args = []) => ({ rowArrays: rows(db, sql, args) }),
      });
      db.exec('COMMIT'); return result;
    } catch (e) { db.exec('ROLLBACK'); throw e; }
    finally { depth--; }
  } };
  return { db, target, depth: () => depth };
}
class ReplicaPort {
  #source; #id;
  constructor(source, id) { this.#source = source; this.#id = id; }
  get receiverId() { this.#source.bindingReads++; return this.#id; }
  async pending({ limit = 100 } = {}) {
    this.#source.events.push(['pending', this.#id]);
    return rows(this.#source.db, `SELECT seq,id,sha,size,n,${this.#id} FROM outgoing WHERE ${this.#id}=0 ORDER BY seq LIMIT ?`, [BigInt(limit)]).map(r => this.#metadata(r));
  }
  #metadata(r) { return { sequence: r[0], deliveryId: r[1], sha256: r[2], byteLength: Number(r[3]), changes: Number(r[4]), acknowledged: r[5] === 1n }; }
  async read(id) {
    this.#source.events.push(['read', this.#id, id]);
    const found = rows(this.#source.db, `SELECT seq,id,sha,size,n,${this.#id},wire FROM outgoing WHERE id=?`, [id]);
    if (!found.length) return null;
    return { delivery: this.#metadata(found[0]), changeset: found[0][5] === 1n ? null : new Uint8Array(found[0][6]) };
  }
  async acknowledge(id, sha) {
    const source = this.#source;
    source.events.push(['ack', this.#id, id]); source.db.exec('BEGIN');
    try {
      const found = rows(source.db, `SELECT sha,${this.#id} FROM outgoing WHERE id=?`, [id]);
      assert.equal(found[0]?.[0], sha, 'source ACK must bind exact retained bytes');
      const changed = found[0][1] === 0n;
      if (changed) source.db.prepare(`UPDATE outgoing SET ${this.#id}=1 WHERE id=?`).run(id);
      source.db.exec('UPDATE outgoing SET wire=zeroblob(0) WHERE east=1 AND west=1');
      source.db.exec('COMMIT'); return changed;
    } catch (e) { source.db.exec('ROLLBACK'); throw e; }
  }
}
async function setup(t, n = 2) {
  const source = { db: new DatabaseSync(':memory:'), events: [], bindingReads: 0, confirms: 0, onConfirm: null };
  t.after(() => source.db.close());
  source.db.exec('CREATE TABLE outgoing(seq INTEGER PRIMARY KEY,id TEXT UNIQUE,sha TEXT,size INTEGER,n INTEGER,east INTEGER,west INTEGER,wire BLOB)');
  const reference = new DatabaseSync(':memory:'); reference.exec(schema);
  try {
    for (let i = 1; i <= n; i++) {
      const session = reference.createSession();
      reference.prepare('INSERT INTO items VALUES(?,?)').run(BigInt(i), `row-${i}`);
      const wire = session.changeset(); session.close();
      source.db.prepare('INSERT INTO outgoing VALUES(?,?,?,?,1,0,0,?)').run(BigInt(i), `source:${i}`, hash(wire), BigInt(wire.length), wire);
    }
  } finally { reference.close(); }
  source.confirm = async () => { source.events.push(['confirm']); source.confirms++; await source.onConfirm?.(); };
  const peers = {};
  for (const id of ['east', 'west']) {
    const sql = owner(t); let confirms = 0;
    const receiver = new ChangesetReceiver(sql.target, { receiverId: id, tables: ['items'], confirmCommit: async () => { assert.equal(sql.depth(), 0); confirms++; } });
    peers[id] = { ...sql, receiver, confirmations: () => confirms };
  }
  const ports = { east: new ReplicaPort(source, 'east'), west: new ReplicaPort(source, 'west') };
  const options = id => ({ receiverId: id, confirmSource: source.confirm,
    deliver: (msg, controls) => peers[id].receiver.receive(msg, controls),
    // Explicit sequential transport: this target tests complete pump acceptance,
    // not an atomic receiver transaction. Every receipt is a real receiver result.
    deliverBatch: async (messages, controls) => {
      const receipts = [];
      for (const msg of messages) receipts.push(await peers[id].receiver.receive(msg, controls));
      return receipts;
    },
  });
  return { source, peers, ports, options,
    pending: id => rows(source.db, `SELECT id FROM outgoing WHERE ${id}=0 ORDER BY seq`).flat(),
    payloads: () => rows(source.db, 'SELECT length(wire) FROM outgoing ORDER BY seq').flat(),
    actual: id => rows(peers[id].db, 'SELECT * FROM items ORDER BY id'),
  };
}

for (const method of ['run', 'runBatch']) {
  test(`${method}: a receipt from east must not consume the west source cursor`, async t => {
    const f = await setup(t);
    await new ChangesetDeliveryPump(f.ports.east, f.options('east'))[method]();
    assert.deepEqual(f.pending('east'), []); assert.equal(f.pending('west').length, 2);
    assert.ok(f.payloads().every(n => n > 0n));
    const before = f.source.events.length, confirmed = f.peers.east.confirmations();
    let rejection;
    try { await new ChangesetDeliveryPump(f.ports.west, f.options('east'))[method](); }
    catch (error) { rejection = error; }
    // On the old implementation both ACK bits become true and wire is reclaimed,
    // even though west has received no rows. This assertion observes real SQL.
    assert.deepEqual(f.pending('west'), ['source:1', 'source:2']);
    assert.deepEqual(f.actual('west'), []);
    assert.ok(f.payloads().every(n => n > 0n));
    assert.ok(code(rejection), 'wrong recipient must reject at construction');
    assert.equal(f.source.events.length, before, 'no confirmation/read/ACK may start');
    assert.equal(f.peers.east.confirmations(), confirmed);
    await new ChangesetDeliveryPump(f.ports.west, f.options('west'))[method]();
    assert.deepEqual(f.actual('west'), f.actual('east'));
    assert.deepEqual(f.payloads(), [0n, 0n]);
  });

  test(`${method}: correct bound ports keep independent progress and exact replay`, async t => {
    const f = await setup(t), east = new ChangesetDeliveryPump(f.ports.east, f.options('east'));
    const first = await east[method](); assert.equal(first.deliveries, 2);
    assert.equal((await east[method]()).deliveries, 0);
    assert.equal(f.pending('west').length, 2); assert.ok(f.payloads().every(n => n > 0n));
    await new ChangesetDeliveryPump(f.ports.west, f.options('west'))[method]();
    assert.deepEqual(f.payloads(), [0n, 0n]);
    assert.equal(f.source.bindingReads, 2, 'capture each trusted source binding once');
  });

  test(`${method}: source methods are captured before mutable adapter replacement`, async t => {
    const f = await setup(t), port = f.ports.east;
    const pump = new ChangesetDeliveryPump(port, f.options('east'));
    for (const name of ['pending', 'read', 'acknowledge']) port[name] = () => assert.fail(`replaced ${name}`);
    Object.defineProperty(port, 'receiverId', { get() { assert.fail('binding reread'); } });
    assert.equal((await pump[method]()).deliveries, 2);
    assert.equal(f.pending('west').length, 2); assert.equal(f.pending('east').length, 0);
    assert.ok(f.payloads().every(n => n > 0n));
  });

  test(`${method}: ACK replacement during receiver confirmation cannot cross recipient state`, async t => {
    const f = await setup(t), entered = gate(), release = gate();
    const original = f.options('east');
    const pump = new ChangesetDeliveryPump(f.ports.east, { ...original,
      deliver: async (...args) => { const response = await original.deliver(...args); entered.resolve(); await release.promise; return response; },
      deliverBatch: async (...args) => { const response = await original.deliverBatch(...args); entered.resolve(); await release.promise; return response; },
    });
    const operation = pump[method]();
    try {
      await Promise.race([entered.promise, operation.then(() => assert.fail('transport did not wait'))]);
      f.ports.east.acknowledge = f.ports.west.acknowledge.bind(f.ports.west);
      release.resolve();
      await operation;
      assert.deepEqual(f.pending('east'), []); assert.equal(f.pending('west').length, 2);
      assert.deepEqual(f.actual('west'), []); assert.ok(f.payloads().every(n => n > 0n));
    } finally { release.resolve(); await Promise.allSettled([operation]); }
  });

  test(`${method}: unbound single-recipient ports remain accepted with private method state`, async t => {
    const f = await setup(t);
    const port = { pending: f.ports.east.pending.bind(f.ports.east), read: f.ports.east.read.bind(f.ports.east), acknowledge: f.ports.east.acknowledge.bind(f.ports.east) };
    assert.equal((await new ChangesetDeliveryPump(port, f.options('east'))[method]()).deliveries, 2);
    assert.equal(f.pending('west').length, 2);
  });
}

test('constructor rejects invalid source ports and bindings without I/O', () => {
  let calls = 0;
  const fn = async () => { calls++; throw new Error('unexpected I/O'); };
  const config = { receiverId: 'east', deliver: fn, deliverBatch: fn, confirmSource: fn };
  const valid = { pending: fn, read: fn, acknowledge: fn };
  for (const id of ['west', 'East', '', null, 1, true, 'east\0', '\ud800', 'x'.repeat(257)])
    assert.throws(() => new ChangesetDeliveryPump({ ...valid, receiverId: id }, config), code);
  for (const source of [null, undefined, {}, { ...valid, pending: null }, { ...valid, read: 1 }, { ...valid, acknowledge: undefined }])
    assert.throws(() => new ChangesetDeliveryPump(source, config), code);
  assert.equal(calls, 0);
});

test('wrong binding rejects before touching method getters', () => {
  let reads = 0;
  const source = { receiverId: 'west' };
  for (const key of ['pending', 'read', 'acknowledge']) Object.defineProperty(source, key, { get() { reads++; throw new Error('method getter'); } });
  assert.throws(() => new ChangesetDeliveryPump(source, { receiverId: 'east' }), code);
  assert.equal(reads, 0);
});

test('source accessors are captured once and called with their original owner', async t => {
  const f = await setup(t); let binding = 0, methods = 0;
  const port = { get receiverId() { binding++; return 'east'; } };
  for (const name of ['pending', 'read', 'acknowledge']) Object.defineProperty(port, name, { get() {
    methods++; return function (...args) { assert.equal(this, port); return f.ports.east[name](...args); };
  } });
  const pump = new ChangesetDeliveryPump(port, f.options('east'));
  await pump.runBatch(); await pump.run();
  assert.equal(binding, 1); assert.equal(methods, 3);
});
