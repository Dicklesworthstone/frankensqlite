import assert from 'node:assert/strict';
import { test } from 'node:test';
import { createServer } from 'node:http';
import { createHash } from 'node:crypto';
import { DatabaseSync } from 'node:sqlite';
import { once } from 'node:events';
import {
  createBootstrapHttpTransport, BootstrapHttpError, BOOTSTRAP_HTTP_PROTOCOL,
  BOOTSTRAP_HTTP_ACTION_HEADER, BOOTSTRAP_HTTP_CONTENT_TYPE, BOOTSTRAP_HTTP_RESPONSE_TYPE,
} from '../src/changeset-bootstrap-http.ts';

const sha = bytes => createHash('sha256').update(bytes).digest('hex');
const hashJson = x => sha(Buffer.from(JSON.stringify(x)));
const protocol = 'fsqlite-bootstrap-v1';
const wait = ms => new Promise(resolve => setTimeout(resolve, ms));
const good = { protocol, receiverId: 'replica-east', deliveryId: 'source:seed', tables: ['t'], chunks: 1, changes: 0, byteLength: 0, sha256: 'a'.repeat(64) };
const full = m => ({ receivedChunks: m.chunks, receivedBytes: m.byteLength, receivedChanges: m.changes, installed: false });
const installed = (m, order) => ({ protocol, receiverId: m.receiverId, deliveryId: m.deliveryId,
  sha256: m.sha256, chunks: m.chunks, changes: m.changes, byteLength: m.byteLength,
  installed: true, confirmed: true, replayed: false, ...(order ? { order } : {}) });
const envelope = (action, result, m = good, index) => ({ protocol: BOOTSTRAP_HTTP_PROTOCOL, action, sha256: m.sha256, result, ...(index === undefined ? {} : { index }) });
const response = value => new Response(JSON.stringify(value), { headers: { 'content-type': BOOTSTRAP_HTTP_RESPONSE_TYPE } });
const client = (fetch, options = {}) => createBootstrapHttpTransport('https://replica.example/bootstrap', { fetch, ...options });
async function server(t, handle) {
  const s = createServer((req, res) => { Promise.resolve(handle(req, res)).catch(error => { if (!res.destroyed) { res.statusCode = 500; res.end('private remote failure'); } }); });
  s.listen(0, '127.0.0.1'); await once(s, 'listening');
  t.after(() => new Promise(resolve => { s.closeAllConnections(); s.close(resolve); }));
  return `http://127.0.0.1:${s.address().port}/bootstrap`;
}
function decodeRequest(bytes) {
  assert.equal(bytes.subarray(0, 4).toString(), 'FCB1');
  const n = bytes.readUInt32BE(4); assert.ok(n > 0 && n <= 131072);
  const info = JSON.parse(bytes.subarray(8, 8 + n).toString());
  assert.equal(info.protocol, BOOTSTRAP_HTTP_PROTOCOL);
  const payload = bytes.subarray(8 + n); assert.equal(payload.length, info.byteLength);
  return { info, payload };
}
async function body(req) { const chunks = []; for await (const c of req) chunks.push(c); return Buffer.concat(chunks); }
function send(res, value) { res.writeHead(200, { 'content-type': BOOTSTRAP_HTTP_RESPONSE_TYPE }); res.end(JSON.stringify(value)); }
function makeSeed() {
  const source = new DatabaseSync(':memory:'); source.exec('CREATE TABLE t(id INTEGER PRIMARY KEY, value, extra);');
  const chunks = [];
  for (const row of [[1n, 'NUL\0€', Buffer.from([0, 1, 255])], [9223372036854775807n, '😀', null]]) {
    const session = source.createSession(); source.prepare('INSERT INTO t VALUES(?,?,?)').run(...row);
    chunks.push(session.changeset()); session.close();
  }
  source.close();
  const m = { ...good, chunks: chunks.length, changes: 2, byteLength: chunks.reduce((n, c) => n + c.length, 0) };
  let chain = hashJson([protocol, m.receiverId, m.deliveryId, m.tables, m.chunks, m.changes, m.byteLength]);
  for (let i = 0; i < chunks.length; i++) chain = hashJson([protocol, chain, i, sha(chunks[i]), chunks[i].length, 1]);
  m.sha256 = chain; return { m, chunks };
}

// An independently implemented native-SQLite endpoint is the client/network oracle.
// This is not a substitute for the SDK receiver in handler integration tests.
test('native SQLite endpoint stages binary chunks, installs once and recovers lost HTTP ACK', async t => {
  const { m, chunks } = makeSeed();
  const db = new DatabaseSync(':memory:'); t.after(() => db.close());
  db.exec('CREATE TABLE t(id INTEGER PRIMARY KEY,value,extra); CREATE TABLE upload(idx INTEGER PRIMARY KEY,payload BLOB);');
  let isInstalled = false, dropped = false, requests = 0;
  const address = await server(t, async (req, res) => {
    requests++; assert.equal(req.headers.authorization, 'Bearer explicit'); assert.equal(req.headers.cookie, undefined);
    assert.equal(req.headers['content-type'], BOOTSTRAP_HTTP_CONTENT_TYPE);
    const { info, payload } = decodeRequest(await body(req));
    assert.equal(info.action, req.headers[BOOTSTRAP_HTTP_ACTION_HEADER]); assert.deepEqual(info.manifest, m);
    if (info.action === 'stage') {
      assert.deepEqual(payload, Buffer.from(chunks[info.index]));
      db.prepare('INSERT OR IGNORE INTO upload VALUES(?,?)').run(info.index, payload);
      const n = db.prepare('SELECT count(*) n FROM upload').get().n;
      assert.equal(db.prepare('SELECT count(*) n FROM t').get().n, isInstalled ? 2 : 0);
      return send(res, envelope('stage', { receivedChunks: n, receivedBytes: chunks.slice(0, n).reduce((x, c) => x + c.length, 0), receivedChanges: n, installed: isInstalled }, m, info.index));
    }
    assert.equal(payload.length, 0);
    if (info.action === 'status') {
      const n = db.prepare('SELECT count(*) n FROM upload').get().n;
      return send(res, envelope('status', n === 0 ? null : { receivedChunks: n, receivedBytes: chunks.slice(0, n).reduce((x, c) => x + c.length, 0), receivedChanges: n, installed: isInstalled }, m));
    }
    assert.equal(info.action, 'install'); const replayed = isInstalled;
    if (!isInstalled) {
      db.exec('BEGIN');
      try { for (const { payload: chunk } of db.prepare('SELECT payload FROM upload ORDER BY idx').all()) assert.equal(db.applyChangeset(chunk), true); db.exec('COMMIT'); isInstalled = true; }
      catch (error) { db.exec('ROLLBACK'); throw error; }
    }
    if (!dropped) { dropped = true; res.destroy(); return; }
    send(res, envelope('install', { ...installed(m), replayed }, m));
  });
  const remote = createBootstrapHttpTransport(address, { allowInsecureLoopback: true, headers: () => ({ authorization: 'Bearer explicit' }) });
  assert.equal(await remote.status(m), null);
  for (let i = 0; i < chunks.length; i++) assert.equal((await remote.stage(m, i, chunks[i])).receivedChunks, i + 1);
  await assert.rejects(remote.install(m), error => error instanceof BootstrapHttpError && error.outcome === 'unknown');
  assert.equal(requests, 4, 'no implicit retry');
  assert.equal((await remote.status(m)).installed, true);
  assert.equal((await remote.install(m)).replayed, true);
  const stmt = db.prepare('SELECT id, CAST(value AS BLOB), extra FROM t ORDER BY id'); stmt.setReadBigInts(true); stmt.setReturnArrays(true);
  assert.deepEqual(stmt.all().map(([id, text, extra]) => [id, new TextDecoder().decode(text), extra]), [[1n, 'NUL\0€', new Uint8Array([0, 1, 255])], [9223372036854775807n, '😀', null]]);
});
test('Fetch policies and action framing are captured before credential provider yields', async () => {
  const m = { ...good, tables: ['T'], byteLength: 3, changes: 1 }, bytes = new Uint8Array([1, 2, 3]);
  const remote = client(async req => {
    assert.equal(req.redirect, 'error'); assert.equal(req.credentials, 'omit'); assert.equal(req.cache, 'no-store');
    assert.equal(req.referrerPolicy, 'no-referrer'); assert.equal(req.mode, 'cors');
    const { info, payload } = decodeRequest(Buffer.from(await req.arrayBuffer()));
    assert.deepEqual([...payload], [1, 2, 3]); assert.deepEqual(info.manifest.tables, ['t']);
    assert.equal(info.manifest.deliveryId, good.deliveryId);
    return response(envelope('stage', full(info.manifest), info.manifest, 0));
  }, { headers: async info => { assert.ok(Object.isFrozen(info.manifest.tables)); m.deliveryId = 'changed'; m.tables[0] = 'changed'; bytes.fill(9); await Promise.resolve(); return { authorization: 'token' }; } });
  assert.equal((await remote.stage(m, 0, bytes)).receivedBytes, 3);
});
for (const [label, options] of [
  ['zero size', { maxChunkBytes: 0 }], ['huge size', { maxChunkBytes: 67108865 }], ['fraction', { maxChunkBytes: 0.5 }],
  ['zero time', { timeoutMs: 0 }], ['null time', { timeoutMs: null }], ['nonboolean HTTP exception', { allowInsecureLoopback: 'yes' }],
  ['invalid fetch', { fetch: 2 }], ['invalid credentials', { headers: {} }], ['bad source identity', { orderedSourceId: '\0' }],
]) test(`client configuration rejects ${label}`, () => assert.throws(() => client(async () => response(null), options)));
for (const url of ['http://replica.example/bootstrap', 'https://u:p@replica.example/x', 'https://replica.example/x#fragment', '/relative', 'file:///tmp/x', 'http://127.0.0.2/']) {
  test(`endpoint refuses ${url}`, () => assert.throws(() => createBootstrapHttpTransport(url, { allowInsecureLoopback: true })));
}
for (const [key, value] of [['protocol', 'fsqlite-changeset-v1'], ['receiverId', ''], ['deliveryId', '\ud800'], ['chunks', 0], ['chunks', 100001], ['changes', -1], ['byteLength', 1073741825], ['sha256', 'A'.repeat(64)], ['tables', []], ['tables', ['t', 'T']], ['tables', ['sqlite_schema']], ['tables', ['__fsqlite_internal']]]) {
  test(`manifest refuses invalid ${key}: ${JSON.stringify(value)}`, async () => {
    let calls = 0; await assert.rejects(client(async () => { calls++; }).status({ ...good, [key]: value }), e => e.outcome === 'not-sent'); assert.equal(calls, 0);
  });
}
test('accessor manifest and sparse or accessor tables are not invoked', async () => {
  let called = 0; const getter = { get() { called++; throw Error('accessor'); } };
  const m = { ...good }; Object.defineProperty(m, 'sha256', getter);
  const remote = client(async () => { called++; }); await assert.rejects(remote.status(m));
  const tables = ['t']; Object.defineProperty(tables, '0', getter); await assert.rejects(remote.status({ ...good, tables }));
  await assert.rejects(remote.status({ ...good, tables: Array(1) })); assert.equal(called, 0);
});
for (const name of ['content-type', 'content-length', 'content-encoding', 'accept', 'cookie', 'host', 'origin', 'connection', 'transfer-encoding', BOOTSTRAP_HTTP_ACTION_HEADER, 'sec-fetch-mode', 'proxy-authorization']) {
  test(`credential provider cannot override ${name}`, async () => {
    let calls = 0; const remote = client(async () => { calls++; }, { headers: () => ({ [name]: 'bad' }) });
    await assert.rejects(remote.status(good), e => e.outcome === 'not-sent'); assert.equal(calls, 0);
  });
}
for (const [label, bytes] of [['shared', new Uint8Array(new SharedArrayBuffer(1))], ['resizable', new Uint8Array(new ArrayBuffer(1, { maxByteLength: 2 }))], ['wrong type', [1]]]) {
  test(`stage refuses ${label} bytes before dispatch`, async () => { let calls = 0; await assert.rejects(client(async () => { calls++; }).stage({ ...good, byteLength: 2 }, 0, bytes)); assert.equal(calls, 0); });
}
test('detached zero-length bytes and negative/fractional/out-of-range stage indices reject', async () => {
  const buffer = new ArrayBuffer(0); const bytes = new Uint8Array(buffer); structuredClone(buffer, { transfer: [buffer] });
  const remote = client(async () => assert.fail('must not dispatch')); await assert.rejects(remote.stage(good, 0, bytes));
  for (const index of [-1, 0.5, 1, NaN]) await assert.rejects(remote.stage(good, index, new Uint8Array()));
});
test('typed-array subclass cannot hide oversize or substitute an iterator', async () => {
  class Sneaky extends Uint8Array { get byteLength() { return 0; } get length() { return 0; } *[Symbol.iterator]() { yield 99; } }
  const remote = client(async () => assert.fail('must not dispatch'), { maxChunkBytes: 1 });
  await assert.rejects(remote.stage({ ...good, byteLength: 2 }, 0, new Sneaky([1, 2])), e => e.code.endsWith('LIMIT') && e.outcome === 'not-sent');
});
for (const [label, value] of [
  ['ordinary receipt', { protocol: 'fsqlite-changeset-v1' }], ['wrong action', envelope('stage', installed(good))],
  ['wrong hash', { ...envelope('install', installed(good)), sha256: 'b'.repeat(64) }],
  ['progress is not an install', envelope('install', { ...full(good), installed: true })],
  ['unconfirmed', envelope('install', { ...installed(good), confirmed: false })],
  ['wrong identity', envelope('install', { ...installed(good), deliveryId: 'other' })],
  ['wrong totals', envelope('install', { ...installed(good), chunks: 2 })],
  ['nonboolean replay', envelope('install', { ...installed(good), replayed: 1 })],
  ['control index', { ...envelope('install', installed(good)), index: 0 }],
]) test(`install refuses ${label}`, async () => assert.rejects(client(async () => response(value)).install(good), e => e.outcome === 'unknown'));
for (const [label, result] of [['missing', null], ['nonadvancing', { receivedChunks: 0, receivedBytes: 0, receivedChanges: 0, installed: false }], ['partial installed', { receivedChunks: 0, receivedBytes: 0, receivedChanges: 0, installed: true }]]) {
  test(`stage refuses ${label} progress`, async () => assert.rejects(client(async () => response(envelope('stage', result, good, 0))).stage(good, 0, new Uint8Array()), e => e.outcome === 'unknown'));
}
test('stage response index must match the submitted chunk', async () => assert.rejects(client(async () => response(envelope('stage', full(good), good, 1))).stage(good, 0, new Uint8Array())));
test('ordered receipt requires the exact configured stream and terminal seed sequence', async () => {
  const order = { protocol: 'fsqlite-ordered-changeset-v1', streamId: 'origin:1', sequence: '1' };
  assert.deepEqual((await client(async () => response(envelope('install', installed(good, order))), { orderedSourceId: 'origin:1' }).install(good)).order, order);
  for (const value of [undefined, { ...order, streamId: 'other' }, { ...order, sequence: '01' }, { ...order, protocol: 'unknown' }]) {
    await assert.rejects(client(async () => response(envelope('install', installed(good, value))), { orderedSourceId: 'origin:1' }).install(good));
  }
  await assert.rejects(client(async () => response(envelope('install', installed(good, order)))).install(good));
});
test('successful responses are detached frozen data', async () => {
  const p = await client(async () => response(envelope('status', full(good)))).status(good); assert.ok(Object.isFrozen(p));
  const r = await client(async () => response(envelope('install', installed(good)))).install(good); assert.ok(Object.isFrozen(r));
});
for (const status of [201, 204, 400, 403, 409, 413, 429, 500, 503]) test(`HTTP ${status} is an unknown outcome without implicit retries`, async () => {
  let calls = 0; const remote = client(async () => { calls++; return new Response(status === 204 ? null : 'sensitive SQL: secret', { status }); });
  await assert.rejects(remote.install(good), e => e.outcome === 'unknown' && e.status === status && !e.message.includes('sensitive'));
  assert.equal(calls, 1);
});
for (const [label, headers, bytes] of [
  ['wrong media', { 'content-type': 'application/json' }, '{}'],
  ['compression', { 'content-encoding': 'gzip' }, '{}'],
  ['bad length', { 'content-length': '-1' }, '{}'],
  ['long declared', { 'content-length': '9000' }, '{}'],
  ['short body', { 'content-length': '10' }, '{}'],
  ['long body', { 'content-length': '1' }, '{}'],
  ['JSON', {}, '{'], ['UTF8', {}, new Uint8Array([0xff])],
]) test(`response refuses ${label}`, async () => {
  const remote = client(async () => new Response(bytes, { headers: { 'content-type': BOOTSTRAP_HTTP_RESPONSE_TYPE, ...headers } }));
  await assert.rejects(remote.status(good), e => e.outcome === 'unknown');
});
test('actual chunked response budget is enforced and reader cancelled', async () => {
  let cancelled = 0; const stream = new ReadableStream({ pull(c) { c.enqueue(new Uint8Array(3000)); }, cancel() { cancelled++; } });
  await assert.rejects(client(async () => new Response(stream, { headers: { 'content-type': BOOTSTRAP_HTTP_RESPONSE_TYPE } })).status(good), e => e.code.endsWith('LIMIT'));
  assert.equal(cancelled, 1); assert.equal(stream.locked, false);
});
test('pre-aborted request does not fetch; credential failure redacts local secret', async () => {
  const c = new AbortController(); c.abort('sensitive reason'); let calls = 0;
  await assert.rejects(client(async () => { calls++; }).status(good, { signal: c.signal }), e => e.outcome === 'not-sent' && e.code.endsWith('CANCELLED'));
  await assert.rejects(client(async () => { calls++; }, { headers: () => { throw Error('secret token'); } }).status(good), e => !e.message.includes('secret') && e.outcome === 'not-sent');
  assert.equal(calls, 0);
});
test('deadline cancels a stalled response and releases its reader', async () => {
  let cancelled = 0; const stream = new ReadableStream({ cancel() { cancelled++; } });
  await assert.rejects(client(async () => new Response(stream, { headers: { 'content-type': BOOTSTRAP_HTTP_RESPONSE_TYPE } }), { timeoutMs: 20 }).status(good), e => e.code.endsWith('TIMEOUT') && e.outcome === 'unknown');
  assert.equal(cancelled, 1); assert.equal(stream.locked, false);
});
test('remaining caller budget shortens configured budget; longer caller budget cannot extend it', async () => {
  for (const [configured, remaining] of [[2000, 15], [15, 2000]]) {
    const remote = client(async req => { await new Promise(resolve => req.signal.addEventListener('abort', resolve, { once: true })); throw Error('abort'); }, { timeoutMs: configured });
    await assert.rejects(remote.status(good, { timeoutMs: remaining }), e => e.code.endsWith('TIMEOUT'));
  }
});
test('custom Fetch ignoring cancellation is drained before rejection', async () => {
  let release, started; const gate = new Promise(resolve => { release = resolve; }); const entered = new Promise(resolve => { started = resolve; });
  const c = new AbortController(); let settled = false;
  const pending = client(async () => { started(); await gate; return response(envelope('status', null)); }).status(good, { signal: c.signal }).finally(() => { settled = true; });
  const rejection = assert.rejects(pending, e => e.outcome === 'unknown'); await entered; c.abort(); await wait(5); assert.equal(settled, false); release(); await rejection;
});
test('redirect is refused without sending explicit credentials to second origin', async t => {
  let hits = 0; const redirected = await server(t, (_req, res) => { hits++; res.end('no'); });
  const address = await server(t, (_req, res) => { res.writeHead(307, { location: redirected }); res.end(); });
  await assert.rejects(createBootstrapHttpTransport(address, { allowInsecureLoopback: true, headers: () => ({ authorization: 'Bearer secret' }) }).status(good), e => e.outcome === 'unknown');
  assert.equal(hits, 0);
});
test('real HTTP stalled receipt times out with unknown outcome', async t => {
  const address = await server(t, async (req, res) => { await body(req); res.writeHead(200, { 'content-type': BOOTSTRAP_HTTP_RESPONSE_TYPE }); res.write('{'); });
  await assert.rejects(createBootstrapHttpTransport(address, { allowInsecureLoopback: true, timeoutMs: 40 }).install(good), e => e.outcome === 'unknown' && e.code.endsWith('TIMEOUT'));
});
