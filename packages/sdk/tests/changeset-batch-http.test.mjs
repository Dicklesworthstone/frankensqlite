// Batch HTTP framing and real reference-SQL receiver integration. Not Rust/WASM
// or power-loss qualification. Real loopback sockets complement stream faults.
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { createHash } from 'node:crypto';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { Readable } from 'node:stream';
import { DatabaseSync } from 'node:sqlite';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import * as http from '../src/changeset-http.ts';
import { ChangesetOrder } from '../src/changeset-order.ts';
import { createOrderedChangesetReceiver, createOrderedChangesetBatchTransport } from '../src/changeset-ordered-delivery.ts';
import { applyChangeset, CHANGESET_RECEIPTS_TABLE } from '../src/changeset-apply.ts';
import { FrankenSQLiteError } from '../src/errors.ts';

const PROTOCOL = 'fsqlite-changeset-v1', ORDER = 'fsqlite-ordered-changeset-v1';
const TYPE = 'application/vnd.fsqlite.ordered-changeset-batch.v1';
const ACK = 'application/vnd.fsqlite.ordered-changeset-batch-receipt.v1+json';
const ROUTE = { receiverId: 'east', sourceId: 'source:epoch-1' };
const DDL = 'CREATE TABLE IF NOT EXISTS notes(id INTEGER PRIMARY KEY,body TEXT UNIQUE,data BLOB);' +
  'CREATE TABLE IF NOT EXISTS decisions(id TEXT PRIMARY KEY,info BLOB);' +
  'CREATE TABLE IF NOT EXISTS parents(id INTEGER PRIMARY KEY);' +
  'CREATE TABLE IF NOT EXISTS children(id INTEGER PRIMARY KEY,pid INTEGER REFERENCES parents);';
const hash = b => createHash('sha256').update(b).digest('hex');
const pause = () => { let resolve; const promise = new Promise(r => { resolve = r; }); return { promise, resolve }; };
const tick = () => new Promise(r => setImmediate(r));
const errorIs = (kind, outcome = 'unknown') => e => e?.code === `ERR_FSQLITE_HTTP_${kind}` && e.outcome === outcome;
function envelope(sequence, bytes, id = `source:${sequence}`) {
  return { protocol: PROTOCOL, receiverId: ROUTE.receiverId, deliveryId: id, sha256: hash(bytes), changeset: bytes,
    order: { protocol: ORDER, streamId: ROUTE.sourceId, sequence: String(sequence) } };
}
function message(sequence = 1n, body = `body-${sequence}`) {
  const db = new DatabaseSync(':memory:'); db.exec(DDL); const s = db.createSession({ table: 'notes' });
  try {
    db.prepare('INSERT INTO notes VALUES(?,?,?)').run(sequence, body, new Uint8Array([0, 255, 7]));
    return envelope(sequence, s.changeset());
  } finally { s.close(); db.close(); }
}
function nativeMessage(sequence, table, sql) {
  const db = new DatabaseSync(':memory:'); db.exec(DDL);
  // Capture only the child table from a consistent source; its parent is sent separately.
  if (table === 'children') db.exec('INSERT INTO parents VALUES(7)');
  const s = db.createSession({ table });
  try { db.exec(sql); return envelope(sequence, s.changeset()); }
  finally { s.close(); db.close(); }
}
const meta = m => ({ protocol: m.protocol, receiverId: m.receiverId, deliveryId: m.deliveryId,
  sha256: m.sha256, byteLength: m.changeset.length, order: { ...m.order } });
const ack = m => ({ ...meta(m), applied: m.changeset.length ? 1 : 0, omitted: 0, replayed: false, confirmed: true });
function frame(messages, transform = x => x) {
  const metadata = transform(messages.map(meta)); const json = new TextEncoder().encode(JSON.stringify(metadata));
  const bytes = new Uint8Array(8 + json.length + messages.reduce((n, m) => n + m.changeset.length, 0));
  bytes.set([70, 67, 66, 49]); new DataView(bytes.buffer).setUint32(4, json.length); bytes.set(json, 8);
  let offset = 8 + json.length; for (const m of messages) { bytes.set(m.changeset, offset); offset += m.changeset.length; }
  return bytes;
}
function request(bytes, headers = {}, signal) {
  return new Request('https://receiver.test/batches', { method: 'POST', headers: {
    'content-type': TYPE, authorization: 'Bearer test', ...headers }, body: bytes, ...(signal ? { signal } : {}), duplex: 'half' });
}
class SqlOwner {
  constructor(path = ':memory:') {
    this.db = new DatabaseSync(path); this.db.exec('PRAGMA foreign_keys=ON;PRAGMA recursive_triggers=ON;' + DDL);
    this.depth = 0; this.serial = 0; this.before = null; this.preCommit = null;
  }
  async transaction(work) {
    const level = this.depth++, sp = `http_${++this.serial}`;
    this.db.exec(level ? `SAVEPOINT ${sp}` : 'BEGIN'); let active = true;
    const check = () => assert.ok(active, 'expired SQL executor');
    const tx = {
      execute: async (sql, params = []) => {
        check(); await this.before?.(sql, level);
        try { return Number(this.db.prepare(sql).run(...params).changes); }
        catch (e) {
          if (e.code === 'ERR_SQLITE_ERROR' && (e.errcode & 255) === 19)
            throw new FrankenSQLiteError({ code: 'SQLITE_CONSTRAINT', sqliteCode: 19, extendedCode: e.errcode, message: e.message });
          throw e;
        }
      },
      query: async (sql, params = []) => {
        check(); const s = this.db.prepare(sql); s.setReadBigInts(true); s.setReturnArrays(true);
        return { rowArrays: s.all(...params) };
      },
      transaction: work => { check(); return this.transaction(work); },
    };
    try { const value = await work(tx); active = false; await this.preCommit?.(level);
      this.db.exec(level ? `RELEASE ${sp}` : 'COMMIT'); return value; }
    catch (e) { this.db.exec(level ? `ROLLBACK TO ${sp}; RELEASE ${sp}` : 'ROLLBACK'); throw e; }
    finally { active = false; this.depth--; }
  }
  rows(sql = 'SELECT id,body,hex(data) FROM notes ORDER BY id') {
    const s = this.db.prepare(sql); s.setReadBigInts(true); s.setReturnArrays(true); return s.all();
  }
  close() { this.db.close(); }
}
async function setup(t, config = {}) {
  const owner = new SqlOwner(config.path); t.after(() => owner.close());
  if (config.wal) owner.db.exec('PRAGMA journal_mode=WAL');
  const order = new ChangesetOrder(owner, ROUTE); await order.initialize();
  const f = { owner, order, applied: [], confirmations: 0, confirm: null, policy: {}, authorizations: [] };
  f.receiver = await createOrderedChangesetReceiver(order, { ...config.receiver,
    apply: (target, m, controls) => {
      f.applied.push(m.deliveryId);
      return applyChangeset(target, m.changeset, { tables: ['notes', 'parents', 'children'],
        deliveryId: m.deliveryId, ...controls, ...f.policy,
        onRebase: (tx, info) => tx.transaction(child => child.execute(
          `INSERT INTO decisions VALUES(?,${info.length ? '?' : 'zeroblob(0)'})`, info.length ? [m.deliveryId, info] : [m.deliveryId])) });
    },
    confirmCommit: async () => { assert.equal(owner.depth, 0); f.confirmations++; await f.confirm?.(); },
  });
  f.handler = http.createOrderedChangesetBatchHttpHandler(f.receiver, {
    authorize: r => r.headers.get('authorization') === 'Bearer test',
    authorizeDelivery: (_r, m) => { f.authorizations.push(m.deliveryId); return true; }, ...config.handler,
  });
  return f;
}
async function serve(t, handler) {
  let calls = 0; const errors = [];
  const server = createServer((q, r) => {
    calls++; const controller = new AbortController();
    q.once('aborted', () => controller.abort());
    r.once('close', () => { if (!r.writableEnded) controller.abort(); });
    const run = async () => {
      const req = new Request(`http://127.0.0.1:${server.address().port}${q.url}`, {
        method: q.method, headers: q.headers, signal: controller.signal,
        ...(q.method !== 'GET' && q.method !== 'HEAD' ? { body: Readable.toWeb(q), duplex: 'half' } : {}),
      });
      const response = await handler(req);
      if (r.destroyed) return;
      r.writeHead(response.status, Object.fromEntries(response.headers));
      r.end(Buffer.from(await response.arrayBuffer()));
    };
    void run().catch(e => { errors.push(e); r.destroy(); });
  });
  server.listen(0, '127.0.0.1'); await once(server, 'listening');
  t.after(async () => { server.closeAllConnections(); await new Promise(r => server.close(r)); assert.deepEqual(errors, []); });
  return { url: `http://127.0.0.1:${server.address().port}/batches`, requests: () => calls };
}
function sender(url, options = {}) {
  return http.createOrderedChangesetBatchHttpTransport(url, { allowInsecureLoopback: true,
    headers: () => ({ authorization: 'Bearer test' }), ...options });
}
const response = value => new Response(JSON.stringify(value), { headers: { 'content-type': ACK } });
async function untouched(f) {
  assert.deepEqual(f.owner.rows(), []); assert.equal((await f.order.head()).sequence, 0n);
  assert.equal(f.confirmations, 0); assert.deepEqual(f.owner.rows('SELECT * FROM decisions'), []);
}

test('batch HTTP: real sockets carry native Session bytes, journal rows and one confirmed receipt array', async t => {
  const f = await setup(t), server = await serve(t, f.handler), messages = [message(1n, 'NUL\0🌍'), message(2n)];
  const send = sender(server.url); const result = await send(messages);
  assert.deepEqual(result, messages.map(ack)); assert.ok(Object.isFrozen(result));
  assert.ok(result.every(r => Object.isFrozen(r) && Object.isFrozen(r.order)));
  assert.equal(server.requests(), 1); assert.equal(f.confirmations, 1);
  assert.equal(f.owner.rows(`SELECT * FROM "${CHANGESET_RECEIPTS_TABLE}"`).length, 2);
  assert.equal(f.owner.rows('SELECT * FROM decisions').length, 2);
  const native = new DatabaseSync(':memory:'); native.exec(DDL);
  try { for (const m of messages) assert.equal(native.applyChangeset(m.changeset), true);
    const s = native.prepare('SELECT id,body,hex(data) FROM notes ORDER BY id'); s.setReadBigInts(true); s.setReturnArrays(true);
    assert.deepEqual(f.owner.rows(), s.all()); } finally { native.close(); }
  const replay = await send(messages); assert.ok(replay.every(r => r.replayed));
  assert.equal(f.applied.length, 2); assert.equal(f.confirmations, 2);
});

test('batch HTTP: retained source sequence checks connect to one real HTTP exchange', async t => {
  const f = await setup(t), server = await serve(t, f.handler), messages = [message(), message(2n)];
  const source = new DatabaseSync(':memory:'); t.after(() => source.close());
  source.exec('CREATE TABLE outgoing(id TEXT PRIMARY KEY, seq INTEGER,hash TEXT,payload BLOB)');
  for (const m of messages) source.prepare('INSERT INTO outgoing VALUES(?,?,?,?)').run(m.deliveryId, BigInt(m.order.sequence), m.sha256, m.changeset);
  const port = { receiverId: 'east', read: async id => {
    assert.equal(f.owner.depth, 0); const stmt = source.prepare('SELECT * FROM outgoing WHERE id=?'); stmt.setReadBigInts(true);
    const row = stmt.get(id); return { delivery: { deliveryId: id, sequence: row.seq, sha256: row.hash,
      byteLength: row.payload.length, changes: 1, acknowledged: false }, changeset: row.payload };
  } };
  const batch = createOrderedChangesetBatchTransport(port, { receiverId: 'east', streamId: ROUTE.sourceId, deliver: sender(server.url) });
  messages[0].order.sequence = '999'; // source-retained sequence is authoritative
  assert.equal((await batch(messages)).length, 2); assert.equal(server.requests(), 1);
  assert.equal(source.prepare('SELECT count(*) n FROM outgoing WHERE length(payload)>0').get().n, 2, 'HTTP must not reclaim source payloads');
});

test('batch HTTP: whole-batch FK policy survives network delivery and nested journal writes', async t => {
  const f = await setup(t, { receiver: { batchForeignKeys: 'defer' } }), server = await serve(t, f.handler);
  const messages = [nativeMessage(1n, 'children', 'INSERT INTO children VALUES(1,7)'), nativeMessage(2n, 'parents', 'INSERT INTO parents VALUES(7)')];
  assert.equal((await sender(server.url)(messages)).length, 2);
  assert.deepEqual(f.owner.rows('PRAGMA foreign_key_check'), []); assert.equal(f.confirmations, 1);
});

test('batch HTTP: a late failed message rolls back rows, decisions, receipts and order', async t => {
  const f = await setup(t), server = await serve(t, f.handler);
  await assert.rejects(sender(server.url)([message(1n, 'same'), message(2n, 'same')]), errorIs('HTTP'));
  await untouched(f);
});

test('batch HTTP: loss after committed confirmation recovers exact identities over reopened WAL', async t => {
  const path = join(mkdtempSync(join(tmpdir(), 'fsqlite-http-batch-')), 'db.sqlite');
  const f = await setup(t, { path, wal: true }); const server = await serve(t, f.handler);
  f.confirm = () => { throw new Error('confirmation response lost'); };
  const messages = [message(), message(2n)];
  await assert.rejects(sender(server.url)(messages), errorIs('HTTP'));
  assert.equal(f.owner.rows().length, 2);
  // Close explicitly without deleting the evidence. Disable the after hook's duplicate close.
  f.owner.close(); f.owner.close = () => {};
  const reopened = await setup(t, { path, wal: true }), again = await serve(t, reopened.handler);
  assert.ok((await sender(again.url)(messages)).every(r => r.replayed));
  assert.equal(reopened.applied.length, 0); assert.equal(reopened.confirmations, 1);
});

test('batch HTTP: authorization of a final member cannot leave an applied prefix', async t => {
  const observed = [];
  const f = await setup(t, { handler: { authorizeDelivery: (_r, m) => { observed.push(m.deliveryId); return m.deliveryId !== 'source:2'; } } });
  const server = await serve(t, f.handler);
  await assert.rejects(sender(server.url)([message(), message(2n)]), e => errorIs('HTTP')(e) && e.status === 403);
  assert.deepEqual(observed, ['source:1', 'source:2']); assert.equal(f.applied.length, 0); await untouched(f);
});

test('batch HTTP: authenticate before pulling unauthorized upload and cancel its body', async t => {
  const f = await setup(t); let pulls = 0, cancelled = 0;
  const stream = new ReadableStream({ pull() { pulls++; }, cancel() { cancelled++; } }, { highWaterMark: 0 });
  const result = await f.handler(request(stream, { authorization: 'wrong' }));
  assert.equal(result.status, 403); assert.equal(pulls, 0); assert.equal(cancelled, 1); await untouched(f);
});

for (const mode of ['missing', 'extra', 'reordered', 'wrong-sequence', 'wrong-digest', 'unconfirmed', 'wrong-type'])
  test(`batch HTTP: ${mode} receipt rejects the entire response, never a prefix`, async () => {
    const messages = [message(), message(2n)], receipts = messages.map(ack);
    const send = sender('https://receiver.test/batches', { fetch: async () => {
      if (mode === 'missing') receipts.pop();
      if (mode === 'extra') receipts.push(ack(message(3n)));
      if (mode === 'reordered') receipts.reverse();
      if (mode === 'wrong-sequence') receipts[1].order.sequence = '3';
      if (mode === 'wrong-digest') receipts[1].sha256 = '0'.repeat(64);
      if (mode === 'unconfirmed') receipts[1].confirmed = false;
      const r = response(receipts); if (mode === 'wrong-type') r.headers.set('content-type', http.CHANGESET_ORDERED_HTTP_RECEIPT_TYPE); return r;
    } });
    await assert.rejects(send(messages), errorIs('PROTOCOL'));
  });

for (const mutation of [
  a => { a[1].order.sequence = '3'; }, a => { a[1].order.streamId = 'another'; },
  a => { a[1].receiverId = 'west'; }, a => { a[1].deliveryId = a[0].deliveryId; },
  a => { a[1].byteLength++; }, a => { a[1].byteLength = -1; }, a => [], a => new Array(257).fill(a[0]),
]) test(`batch HTTP: invalid complete metadata ${String(mutation)} rejects before SQL`, async t => {
  const f = await setup(t), raw = frame([message(), message(2n)], values => mutation(values) ?? values);
  assert.equal((await f.handler(request(raw))).status, 400); assert.equal(f.applied.length, 0); await untouched(f);
});

test('batch HTTP: format confusion, malformed metadata, truncation and trailing data fail closed', async t => {
  const f = await setup(t), valid = frame([message(), message(2n)]);
  const badMagic = valid.slice(); badMagic[2] = 79;
  const oversize = valid.slice(); new DataView(oversize.buffer).setUint32(4, 0xffffffff);
  const trailing = new Uint8Array(valid.length + 1); trailing.set(valid);
  const invalidJson = valid.slice(); invalidJson[8] = 0xff;
  for (const wire of [badMagic, oversize, trailing, invalidJson, valid.slice(0, 7), valid.slice(0, -1)]) {
    assert.equal((await f.handler(request(wire))).status, 400); assert.equal(f.applied.length, 0);
  }
  await untouched(f);
});

test('batch HTTP: actual streamed bytes obey absent and dishonest content lengths', async t => {
  const f = await setup(t, { handler: { maxBatchBytes: 1 } }), messages = [message(), message(2n)];
  const raw = frame(messages);
  const streamOf = bytes => new ReadableStream({ start(c) { for (let p = 0; p < bytes.length; p += 3) c.enqueue(bytes.slice(p, p + 3)); c.close(); } });
  assert.equal((await f.handler(request(streamOf(raw)))).status, 413);
  assert.equal((await f.handler(request(streamOf(raw), { 'content-length': '1' }))).status, 413);
  assert.equal((await f.handler(request(streamOf(raw), { 'content-length': String(raw.length + 1) }))).status, 400);
  assert.equal((await f.handler(request(raw, { 'content-encoding': 'gzip' }))).status, 400);
  await untouched(f);
});

test('batch HTTP: exact aggregate bounds count intrinsic byte sizes before credentials or fetch', async t => {
  const messages = [message(), message(2n)], total = messages.reduce((n, m) => n + m.changeset.length, 0);
  let external = 0, getters = 0;
  class Hidden extends Uint8Array { get byteLength() { getters++; return 0; } }
  messages[1].changeset = new Hidden(messages[1].changeset);
  const limited = sender('https://receiver.test/batches', { maxBatchBytes: total - 1,
    headers: () => { external++; return {}; }, fetch: async () => { external++; return response([]); } });
  await assert.rejects(limited(messages), errorIs('LIMIT', 'not-sent')); assert.equal(getters, 0); assert.equal(external, 0);
  const f = await setup(t, { handler: { maxBatchBytes: total } });
  const send = sender('https://receiver.test/batches', { maxBatchBytes: total, fetch: f.handler });
  assert.equal((await send(messages)).length, 2);
});

test('batch HTTP: 256 empty messages retain all identities and require only one confirmation', async t => {
  const f = await setup(t), messages = Array.from({ length: 256 }, (_, i) => envelope(BigInt(i + 1), new Uint8Array()));
  const server = await serve(t, f.handler);
  const receipt = await sender(server.url, { maxBatchBytes: 1 })(messages);
  assert.equal(receipt.length, 256); assert.equal(f.confirmations, 1); assert.equal((await f.order.head()).sequence, 256n);
});

test('batch HTTP: source identities beyond safe integer range remain exact through JSON framing', async () => {
  for (const first of [9007199254740993n, (1n << 63n) - 2n]) {
    const messages = [envelope(first, new Uint8Array()), envelope(first + 1n, new Uint8Array())];
    const send = sender('https://receiver.test/batches', { fetch: async req => {
      const wire = new Uint8Array(await req.arrayBuffer()), size = new DataView(wire.buffer).getUint32(4);
      const metadata = JSON.parse(new TextDecoder().decode(wire.subarray(8, 8 + size)));
      assert.equal(metadata[1].order.sequence, String(first + 1n));
      return response(messages.map(ack));
    } });
    assert.equal((await send(messages))[1].order.sequence, String(first + 1n));
  }
});

test('batch HTTP: input and credentials metadata are privately captured before the first await', async t => {
  const f = await setup(t), gate = pause(), entered = pause(), messages = [message(), message(2n)];
  const expected = messages.map(ack);
  const options = { fetch: f.handler, headers: async (metas, signal) => {
    assert.ok(Object.isFrozen(metas)); assert.ok(metas.every(m => Object.isFrozen(m) && !('changeset' in m)));
    assert.ok(signal instanceof AbortSignal); entered.resolve(); await gate.promise; return { authorization: 'Bearer test' };
  } };
  const send = sender('https://receiver.test/batches', options), operation = send(messages);
  try {
    await entered.promise;
    options.fetch = () => assert.fail('configuration changed');
    for (const m of messages) { m.changeset.fill(0); structuredClone(m.changeset.buffer, { transfer: [m.changeset.buffer] }); m.order.sequence = '99'; }
    messages.length = 0; gate.resolve(); assert.deepEqual(await operation, expected);
  } finally { gate.resolve(); await Promise.allSettled([operation]); }
});

test('batch HTTP: sparse/accessor arrays and unsafe buffers cannot invoke credentials', async () => {
  let invoked = 0;
  const send = sender('https://receiver.test/batches', { headers: () => { invoked++; return {}; }, fetch: async () => response([]) });
  const accessor = [message()]; Object.defineProperty(accessor, '0', { get() { invoked++; return message(); } });
  for (const input of [[], new Array(1), new Array(257), accessor, { *[Symbol.iterator]() { invoked++; yield message(); } }])
    await assert.rejects(send(input), e => e.outcome === 'not-sent');
  for (const bytes of [new Uint8Array(new SharedArrayBuffer(4)), new Uint8Array(new ArrayBuffer(4, { maxByteLength: 8 }))])
    await assert.rejects(send([{ ...message(), changeset: bytes }]), errorIs('INPUT', 'not-sent'));
  assert.equal(invoked, 0);
});

for (const ending of ['cancel', 'timeout']) test(`batch HTTP: ${ending} holds server capacity until committed confirmation settles`, async t => {
  const f = await setup(t), entered = pause(), release = pause(), controller = new AbortController();
  f.confirm = async () => { entered.resolve(); await release.promise; };
  const handler = http.createOrderedChangesetBatchHttpHandler(f.receiver, { authorize: () => true, timeoutMs: ending === 'timeout' ? 180 : 5000 });
  const wire = frame([message(), message(2n)]), operation = handler(request(wire, {}, controller.signal));
  let done = false; void operation.then(() => { done = true; });
  try {
    await Promise.race([entered.promise, operation.then(() => assert.fail('no confirmation'))]);
    if (ending === 'cancel') controller.abort(); else await new Promise(r => setTimeout(r, 220));
    await tick(); assert.equal(done, false);
    assert.equal((await handler(request(wire))).status, 503);
    release.resolve();
    // The existing handler maps ordered-receiver cancellation to a generic 500;
    // keep that sanitized unknown outcome, not a claim that committed SQL aborted.
    assert.equal((await operation).status, 500);
    assert.equal(f.owner.rows().length, 2); assert.equal((await f.order.head()).sequence, 2n);
    f.confirm = null;
    assert.ok((await (await handler(request(wire))).json()).every(r => r.replayed));
  } finally { release.resolve(); await Promise.allSettled([operation]); }
});

test('batch HTTP: client cancellation waits for admitted fetch and reports uncertain outcome', async () => {
  const entered = pause(), release = pause(), controller = new AbortController(), messages = [message(), message(2n)];
  const send = sender('https://receiver.test/batches', { fetch: async req => { assert.equal(req.redirect, 'error'); entered.resolve(); await release.promise; return response(messages.map(ack)); } });
  const operation = send(messages, { signal: controller.signal }); let done = false;
  void operation.then(() => { done = true; }, () => { done = true; });
  try { await entered.promise; controller.abort(); await tick(); assert.equal(done, false);
    release.resolve(); await assert.rejects(operation, errorIs('CANCELLED')); }
  finally { release.resolve(); await Promise.allSettled([operation]); }
});

test('batch HTTP: response parser bounds streamed bytes and waits for cancellation cleanup', async () => {
  const entered = pause(), release = pause(); let cancelled = false;
  const body = new ReadableStream({ start(c) { c.enqueue(new Uint8Array(256 * 8192 + 258)); },
    async cancel() { cancelled = true; entered.resolve(); await release.promise; } });
  const send = sender('https://receiver.test/batches', { fetch: async () => new Response(body, { headers: { 'content-type': ACK } }) });
  const operation = send([message()]); let done = false; void operation.catch(() => { done = true; });
  try { await entered.promise; assert.equal(cancelled, true); await tick(); assert.equal(done, false);
    release.resolve(); await assert.rejects(operation, errorIs('LIMIT')); }
  finally { release.resolve(); await Promise.allSettled([operation]); }
});

test('batch HTTP: HTTPS, no ambient credentials, no redirects, and explicit loopback exception are retained', async () => {
  for (const url of ['http://example.com/batch', 'https://user:pass@example.com/batch', 'https://example.com/batch#fragment'])
    assert.throws(() => sender(url), { code: 'ERR_FSQLITE_HTTP_INPUT' });
  let sends = 0;
  const messages = [message()]; const send = sender('https://receiver.test/batches', { fetch: async req => {
    sends++; assert.equal(req.method, 'POST'); assert.equal(req.redirect, 'error'); assert.equal(req.credentials, 'omit');
    assert.equal(req.cache, 'no-store'); assert.equal(req.referrerPolicy, 'no-referrer');
    return new Response(null, { status: 307, headers: { location: 'https://elsewhere.test/' } });
  } });
  await assert.rejects(send(messages), e => errorIs('HTTP')(e) && e.status === 307); assert.equal(sends, 1);
  for (const name of ['cookie', 'content-type', 'host', 'transfer-encoding', 'sec-example', 'proxy-example'])
    await assert.rejects(sender('https://receiver.test/batches', { headers: () => ({ [name]: 'bad' }), fetch: async () => assert.fail('sent') })(messages), errorIs('INPUT', 'not-sent'));
});

test('batch HTTP: CORS preflight and wrong media-type do not read body or invoke SQL', async t => {
  const f = await setup(t, { handler: { allowedOrigins: ['https://client.test'] } });
  const headers = { origin: 'https://client.test', 'access-control-request-method': 'POST', 'access-control-request-headers': 'content-type, authorization' };
  const allowed = await f.handler(new Request('https://receiver.test/batches', { method: 'OPTIONS', headers }));
  assert.equal(allowed.status, 204); assert.equal(allowed.headers.get('access-control-allow-origin'), 'https://client.test');
  const denied = await f.handler(new Request('https://receiver.test/batches', { method: 'OPTIONS', headers: { ...headers, origin: 'https://untrusted.test' } }));
  assert.equal(denied.status, 403);
  assert.equal((await f.handler(request(frame([message()]), { 'content-type': http.CHANGESET_ORDERED_HTTP_CONTENT_TYPE }))).status, 415);
  assert.equal(f.applied.length, 0);
});

for (const ordered of [false, true]) test(`shared HTTP exchange preserves ${ordered ? 'ordered' : 'legacy'} single framing and receipt behavior`, async () => {
  const value = message(); if (!ordered) delete value.order;
  const make = ordered ? http.createOrderedChangesetHttpTransport : http.createChangesetHttpTransport;
  const handler = ordered ? http.createOrderedChangesetHttpHandler : http.createChangesetHttpHandler;
  const receiver = { receiverId: ROUTE.receiverId, streamId: ROUTE.sourceId, receive: async m => ({ ...ack({ ...m, order: value.order }), ...(ordered ? {} : { order: undefined }) }) };
  if (!ordered) receiver.receive = async m => { const result = ack({ ...m, order: {} }); delete result.order; return result; };
  const endpoint = handler(receiver, { authorize: () => true });
  const send = make('https://receiver.test/single', { fetch: async req => {
    const wire = new Uint8Array(await req.clone().arrayBuffer()); assert.equal(wire[2], ordered ? 79 : 68);
    return endpoint(req);
  } });
  const result = await send(value); assert.equal(result.deliveryId, value.deliveryId); assert.equal(result.confirmed, true);
  const expected = ack({ ...value, order: value.order ?? {} }); if (!ordered) delete expected.order;
  assert.deepEqual(result, expected);
});

test('batch HTTP: bad final digest or truncated Session record never reaches application SQL', async t => {
  const f = await setup(t), messages = [message(), message(2n)];
  const badDigest = { ...messages[1], sha256: '0'.repeat(64) };
  const truncated = envelope(2n, messages[1].changeset.slice(0, -1));
  for (const last of [badDigest, truncated]) {
    const server = await serve(t, f.handler);
    await assert.rejects(sender(server.url)([messages[0], last]), errorIs('HTTP'));
    assert.equal(f.applied.length, 0); await untouched(f);
  }
});

test('batch HTTP: abort during held journal write drains SQL before rollback and slot release', async t => {
  const f = await setup(t), entered = pause(), release = pause(), controller = new AbortController();
  let held = false;
  f.owner.before = async sql => {
    if (!held && sql.startsWith('INSERT INTO decisions')) { held = true; entered.resolve(); await release.promise; }
  };
  const wire = frame([message(), message(2n)]), operation = f.handler(request(wire, {}, controller.signal));
  let done = false; void operation.then(() => { done = true; });
  try {
    await Promise.race([entered.promise, operation.then(() => assert.fail('journal did not run'))]);
    controller.abort(); await tick(); assert.equal(done, false); assert.ok(f.owner.depth > 0);
    assert.equal((await f.handler(request(wire))).status, 503);
    release.resolve(); assert.equal((await operation).status, 500);
    assert.equal(f.applied.length, 1); await untouched(f);
    f.owner.before = null;
    assert.equal((await f.handler(request(wire))).status, 200);
  } finally { release.resolve(); await Promise.allSettled([operation]); }
});

test('batch HTTP: aborted real socket leaves admission closed until receiver confirmation settles', async t => {
  const f = await setup(t), entered = pause(), release = pause(), settled = pause();
  let firstRequest = true;
  const server = await serve(t, async request => {
    const first = firstRequest; firstRequest = false;
    try { return await f.handler(request); } finally { if (first) settled.resolve(); }
  });
  const controller = new AbortController(), messages = [message(), message(2n)];
  f.confirm = async () => { entered.resolve(); await release.promise; };
  const operation = sender(server.url)(messages, { signal: controller.signal });
  const rejected = assert.rejects(operation, errorIs('CANCELLED'));
  try {
    await entered.promise; controller.abort(); await rejected;
    assert.equal(f.owner.rows().length, 2);
    await assert.rejects(sender(server.url)(messages), e => errorIs('HTTP')(e) && e.status === 503);
    release.resolve(); await settled.promise; f.confirm = null;
    // The aborted socket has no receipt, not evidence of rollback. The same
    // messages recover the complete stored outcome, never a new application.
    const receipts = await sender(server.url)(messages);
    assert.ok(receipts.every(r => r.replayed)); assert.equal(f.applied.length, 2);
  } finally { release.resolve(); await rejected; }
});

test('batch HTTP: application diagnostics and credential secrets never enter the remote error body', async t => {
  const f = await setup(t), secret = 'token-and-private-sql-diagnostic';
  const handler = http.createOrderedChangesetBatchHttpHandler({
    receiverId: ROUTE.receiverId, streamId: ROUTE.sourceId,
    receiveBatch: async () => { throw new Error(secret); },
  }, { authorize: () => true });
  const received = await handler(request(frame([message()])));
  assert.equal(received.status, 500);
  assert.deepEqual(await received.json(), { error: 'ERR_FSQLITE_HTTP_REJECTED', outcome: 'unknown' });
  const denied = http.createOrderedChangesetBatchHttpHandler(f.receiver, { authorize: () => { throw new Error(secret); } });
  const rejection = await denied(request(frame([message()])));
  assert.equal(rejection.status, 403); assert.ok(!(await rejection.text()).includes(secret));
  await untouched(f);
});

test('batch HTTP: a forged last receiver result never emits partial success after application', async t => {
  const f = await setup(t), messages = [message(), message(2n)];
  const handler = http.createOrderedChangesetBatchHttpHandler({ receiverId: ROUTE.receiverId, streamId: ROUTE.sourceId,
    receiveBatch: async (batch, controls) => {
      const receipts = await f.receiver.receiveBatch(batch, controls);
      return [receipts[0], { ...receipts[1], confirmed: false }];
    },
  }, { authorize: () => true });
  const server = await serve(t, handler);
  await assert.rejects(sender(server.url)(messages), errorIs('HTTP'));
  assert.equal(f.owner.rows().length, 2); assert.equal(f.confirmations, 1);
  assert.equal((await f.order.head()).sequence, 2n);
});
