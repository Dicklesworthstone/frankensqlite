import assert from 'node:assert/strict';
import { test } from 'node:test';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { createHash } from 'node:crypto';
import { DatabaseSync } from 'node:sqlite';
import { Readable } from 'node:stream';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawn } from 'node:child_process';
import { ChangesetReceiver, ChangesetDeliveryPump } from '../src/changeset-delivery.ts';
import { ChangesetOutbox } from '../src/changeset-outbox.ts';
import {
  createChangesetHttpTransport, createChangesetHttpHandler, ChangesetHttpError,
  CHANGESET_HTTP_CONTENT_TYPE as REQUEST_TYPE,
  CHANGESET_HTTP_RECEIPT_TYPE as RECEIPT_TYPE,
} from '../src/changeset-http.ts';

const hash = bytes => createHash('sha256').update(bytes).digest('hex');
function message(bytes = new Uint8Array(), id = 'source:1', receiverId = 'receiver') {
  return { protocol: 'fsqlite-changeset-v1', receiverId, deliveryId: id, sha256: hash(bytes), changeset: bytes };
}
function ack(meta, extra = {}) {
  return { protocol: meta.protocol, receiverId: meta.receiverId, deliveryId: meta.deliveryId,
    sha256: meta.sha256, byteLength: meta.byteLength ?? meta.changeset.length,
    applied: 0, omitted: 0, replayed: false, confirmed: true, ...extra };
}
function frame(bytes) {
  const data = new Uint8Array(bytes);
  assert.deepEqual([...data.subarray(0, 4)], [70, 67, 68, 49]);
  const count = new DataView(data.buffer, data.byteOffset, data.byteLength).getUint32(4);
  const meta = JSON.parse(new TextDecoder().decode(data.subarray(8, 8 + count)));
  return { meta, bytes: data.subarray(8 + count) };
}
async function server(t, handler) {
  const errors = [];
  const http = createServer((q, r) => { Promise.resolve(handler(q, r)).catch(e => {
    errors.push(e); r.destroy();
  }); });
  http.listen(0, '127.0.0.1'); await once(http, 'listening');
  t.after(async () => { http.closeAllConnections(); await new Promise(resolve => http.close(resolve)); assert.deepEqual(errors, []); });
  return `http://127.0.0.1:${http.address().port}/changes`;
}
const read = async q => { const parts = []; for await (const part of q) parts.push(part); return Buffer.concat(parts); };
const jsonReply = (r, receipt, status = 200) => { r.writeHead(status, { 'content-type': RECEIPT_TYPE }); r.end(JSON.stringify(receipt)); };
function client(url, options = {}) { return createChangesetHttpTransport(url, { allowInsecureLoopback: true, ...options }); }
function isError(code, outcome = 'unknown') {
  return e => e instanceof ChangesetHttpError && e.code === `ERR_FSQLITE_HTTP_${code}` && e.outcome === outcome;
}

// An independent C SQLite endpoint is an interoperability oracle for the binary
// transport, not a fake FrankenSQLite engine. Handler integration follows below.
test('native SQLite session bytes cross real HTTP unchanged and replay safely', async t => {
  const source = new DatabaseSync(':memory:'), destination = new DatabaseSync(':memory:');
  t.after(() => { source.close(); destination.close(); });
  const schema = 'CREATE TABLE notes(id INTEGER PRIMARY KEY, body TEXT, data BLOB)';
  source.exec(schema); destination.exec(schema);
  destination.exec('CREATE TABLE inbox(id TEXT PRIMARY KEY, digest TEXT NOT NULL)');
  const session = source.createSession();
  source.prepare('INSERT INTO notes VALUES(?,?,?)').run(9223372036854775807n, 'a\0🌍', new Uint8Array([0, 255, 1]));
  const bytes = session.changeset(); session.close();
  const envelope = message(bytes, '来源:事件/1', 'récepteur🌍');
  let requests = 0;
  const url = await server(t, async (q, r) => {
    requests++;
    assert.equal(q.method, 'POST'); assert.equal(q.headers['content-type'], REQUEST_TYPE);
    assert.equal(q.headers.authorization, 'Bearer test-only'); assert.equal(q.headers.cookie, undefined);
    const received = frame(await read(q));
    assert.deepEqual(Buffer.from(received.bytes), Buffer.from(bytes));
    assert.equal(hash(received.bytes), received.meta.sha256);
    const prior = destination.prepare('SELECT digest FROM inbox WHERE id=?').get(received.meta.deliveryId);
    if (!prior) {
      destination.exec('BEGIN');
      assert.equal(destination.applyChangeset(received.bytes), true);
      destination.prepare('INSERT INTO inbox VALUES (?,?)').run(received.meta.deliveryId, received.meta.sha256);
      destination.exec('COMMIT');
    } else assert.equal(prior.digest, received.meta.sha256);
    jsonReply(r, ack(received.meta, { applied: 1, replayed: !!prior }));
  });
  const deliver = client(url, { headers: () => ({ authorization: 'Bearer test-only' }) });
  assert.equal((await deliver(envelope, {})).replayed, false);
  assert.equal((await deliver(envelope, {})).replayed, true);
  assert.equal(requests, 2);
  const row = destination.prepare('SELECT CAST(id AS TEXT) AS id, hex(body) AS body, hex(data) AS data FROM notes').get();
  assert.deepEqual({ ...row }, { id: '9223372036854775807', body: '6100F09F8C8D', data: '00FF01' });
});

test('payload and routing fields are captured before asynchronous credentials', async t => {
  const bytes = new Uint8Array([0, 1, 2, 255]), original = bytes.slice(), envelope = message(bytes);
  const url = await server(t, async (q, r) => {
    const got = frame(await read(q)); assert.deepEqual(got.bytes, original);
    assert.equal(got.meta.deliveryId, 'source:1'); jsonReply(r, ack(got.meta));
  });
  await client(url, { headers: async meta => {
    assert.ok(Object.isFrozen(meta)); bytes.fill(9); envelope.deliveryId = 'other';
    await Promise.resolve(); return {};
  } })(envelope, {});
});

test('largest valid escaped identities fit the frame and receipt budget', async t => {
  const envelope = message(new Uint8Array(), '\u0001'.repeat(512), '\u0002'.repeat(256));
  const url = await server(t, async (q, r) => jsonReply(r, ack(frame(await read(q)).meta)));
  assert.equal((await client(url)(envelope, {})).deliveryId, envelope.deliveryId);
});

test('intrinsic buffer capture ignores subclass getters and iterators', async t => {
  class Bytes extends Uint8Array {
    get byteLength() { throw Error('getter'); }
    get buffer() { throw Error('getter'); }
    [Symbol.iterator]() { throw Error('iterator'); }
  }
  const bytes = new Bytes([1, 2]); const envelope = { ...message(new Uint8Array([1, 2])), changeset: bytes };
  const url = await server(t, async (q, r) => { const data = frame(await read(q)); assert.deepEqual([...data.bytes], [1, 2]); jsonReply(r, ack(data.meta)); });
  await client(url)(envelope, {});
});

for (const url of ['http://example.com/a', 'https://u:p@example.com/a', 'https://example.com/#x',
  'file:///tmp/db', 'data:text/plain,x', 'https://example.com@evil.test/#x']) {
  test(`reject unsafe endpoint ${url} without making a request`, () => {
    assert.throws(() => createChangesetHttpTransport(url, { allowInsecureLoopback: true }), isError('INPUT', 'not-sent'));
  });
}
test('loopback HTTP requires explicit opt-in', () => {
  assert.throws(() => createChangesetHttpTransport('http://127.0.0.1/x'), isError('INPUT', 'not-sent'));
  assert.throws(() => createChangesetHttpTransport('http://127.0.0.1.evil.test/x', { allowInsecureLoopback: true }), isError('INPUT', 'not-sent'));
});

for (const name of ['content-type', 'content-length', 'content-encoding', 'cookie', 'host', 'origin', 'transfer-encoding', 'sec-fetch-site', 'proxy-authorization']) {
  test(`credentials cannot override ${name}`, async () => {
    let calls = 0;
    const deliver = createChangesetHttpTransport('https://example.test/x', { headers: () => ({ [name]: 'x' }), fetch: async () => { calls++; throw Error('must not send'); } });
    await assert.rejects(deliver(message(), {}), isError('INPUT', 'not-sent')); assert.equal(calls, 0);
  });
}

test('fetch receives explicit no-cookie/no-cache/no-redirect policies', async () => {
  await createChangesetHttpTransport('https://example.test/x', { fetch: async request => {
    assert.equal(request.method, 'POST'); assert.equal(request.credentials, 'omit'); assert.equal(request.cache, 'no-store');
    assert.equal(request.redirect, 'error'); assert.equal(request.referrerPolicy, 'no-referrer'); assert.equal(request.mode, 'cors');
    const { meta } = frame(await request.arrayBuffer());
    return new Response(JSON.stringify(ack(meta)), { headers: { 'content-type': RECEIPT_TYPE } });
  } })(message(), {});
});

test('real redirect never forwards a payload or credentials to another endpoint', async t => {
  let leaked = 0;
  const other = await server(t, (q, r) => { leaked++; r.end(); });
  const first = await server(t, async (q, r) => { await read(q); r.writeHead(307, { location: other }); r.end(); });
  await assert.rejects(client(first, { headers: () => ({ authorization: 'secret' }) })(message(), {}), isError('NETWORK'));
  assert.equal(leaked, 0);
});

for (const status of [202, 204, 400, 401, 409, 429, 500, 503]) {
  test(`HTTP ${status} cannot become a success or an automatic retry`, async t => {
    let calls = 0;
    const url = await server(t, async (q, r) => { calls++; await read(q); r.writeHead(status); r.end('private SQL detail'); });
    await assert.rejects(client(url)(message(), {}), e => isError('HTTP')(e) && e.status === status && !e.message.includes('private'));
    assert.equal(calls, 1);
  });
}
for (const [key, value] of Object.entries({ protocol: 'old', receiverId: 'other', deliveryId: 'other', sha256: '0'.repeat(64), byteLength: 10,
  confirmed: false, applied: -1, omitted: 100001, replayed: 'yes' })) {
  test(`reject mismatched receipt ${key}`, async t => {
    const url = await server(t, async (q, r) => jsonReply(r, ack(frame(await read(q)).meta, { [key]: value })));
    await assert.rejects(client(url)(message(), {}), isError('PROTOCOL'));
  });
}

test('chunked receipt is bounded by actual bytes without Content-Length', async t => {
  const url = await server(t, async (q, r) => { await read(q); r.writeHead(200, { 'content-type': RECEIPT_TYPE }); r.write(' '.repeat(8192)); r.end('x'); });
  await assert.rejects(client(url)(message(), {}), isError('LIMIT'));
});
test('oversized declared receipt is cancelled before being read', async () => {
  let cancelled = false;
  const deliver = createChangesetHttpTransport('https://example.test/x', { fetch: async () => new Response(new ReadableStream({ cancel() { cancelled = true; } }),
    { headers: { 'content-type': RECEIPT_TYPE, 'content-length': '8193' } }) });
  await assert.rejects(deliver(message(), {}), isError('LIMIT')); assert.equal(cancelled, true);
});
test('a lying or truncated Content-Length never yields a receipt', async () => {
  for (const length of ['1', '4096', '-1', '2, 2', '00']) {
    const deliver = createChangesetHttpTransport('https://example.test/x', { fetch: async () => new Response(JSON.stringify(ack(message())),
      { headers: { 'content-type': RECEIPT_TYPE, 'content-length': length } }) });
    await assert.rejects(deliver(message(), {}), e => e instanceof ChangesetHttpError && e.outcome === 'unknown');
  }
});
for (const body of ['{', 'null', '[]', '{"confirmed":true}', new Uint8Array([0xff])]) {
  test(`malformed receipt ${String(body).slice(0, 24)} rejects`, async () => {
    const deliver = createChangesetHttpTransport('https://example.test/x', { fetch: async () => new Response(body, { headers: { 'content-type': RECEIPT_TYPE } }) });
    await assert.rejects(deliver(message(), {}), isError('PROTOCOL'));
  });
}
test('wrong receipt media type and compressed responses reject', async () => {
  for (const headers of [{ 'content-type': 'text/html' }, { 'content-type': RECEIPT_TYPE, 'content-encoding': 'gzip' }]) {
    const deliver = createChangesetHttpTransport('https://example.test/x', { fetch: async () => new Response('{}', { headers }) });
    await assert.rejects(deliver(message(), {}), isError('PROTOCOL'));
  }
});

test('pre-aborted calls and invalid/oversized payloads never dispatch', async () => {
  let calls = 0;
  const deliver = createChangesetHttpTransport('https://example.test/x', { maxMessageBytes: 1, fetch: async () => { calls++; throw Error(); } });
  await assert.rejects(deliver(message(), { signal: AbortSignal.abort('stop') }), isError('CANCELLED', 'not-sent'));
  await assert.rejects(deliver(message(new Uint8Array(2)), {}), isError('LIMIT', 'not-sent'));
  for (const changeset of [new Uint8Array(new SharedArrayBuffer(1)), new Uint8Array(new ArrayBuffer(1, { maxByteLength: 2 }))]) {
    await assert.rejects(deliver({ ...message(), changeset }, {}), isError('INPUT', 'not-sent'));
  }
  const detached = new Uint8Array(0); structuredClone(detached, { transfer: [detached.buffer] });
  await assert.rejects(deliver({ ...message(), changeset: detached }, {}), isError('INPUT', 'not-sent'));
  assert.equal(calls, 0);
});
test('deadline covers slow headers and signals real fetch cancellation', async t => {
  let observed;
  const started = new Promise(resolve => { observed = resolve; });
  const url = await server(t, async (q, r) => { await read(q); observed(); /* deliberately withhold response */ });
  const result = client(url, { timeoutMs: 60 })(message(), {});
  const failed = assert.rejects(result, isError('TIMEOUT'));
  await started; await failed;
});
test('stalled chunked receipt respects timeout and releases its stream', async t => {
  const url = await server(t, async (q, r) => { await read(q); r.writeHead(200, { 'content-type': RECEIPT_TYPE }); r.write('{'); });
  await assert.rejects(client(url, { timeoutMs: 60 })(message(), {}), isError('TIMEOUT'));
});
test('external cancellation after dispatch remains an unknown outcome', async t => {
  const controller = new AbortController();
  const url = await server(t, async (q, r) => { await read(q); controller.abort('lost interest'); });
  await assert.rejects(client(url)(message(), { signal: controller.signal }), isError('CANCELLED'));
});
test('credential delay drains before rejecting; no delayed request escapes cancellation', async () => {
  const controller = new AbortController(); let calls = 0;
  const deliver = createChangesetHttpTransport('https://example.test/x', { headers: async (_meta, signal) => {
    controller.abort(); await Promise.resolve(); assert.equal(signal.aborted, true); return {};
  }, fetch: async () => { calls++; throw Error(); } });
  await assert.rejects(deliver(message(), { signal: controller.signal }), isError('CANCELLED', 'not-sent')); assert.equal(calls, 0);
});
test('a lost response never triggers implicit retry and explicit retry preserves identity', async t => {
  let committed = false, writes = 0, attempts = 0;
  const url = await server(t, async (q, r) => {
    const { meta } = frame(await read(q)); attempts++;
    const replayed = committed;
    if (!committed) { committed = true; writes++; }
    if (attempts === 1) r.destroy(); else jsonReply(r, ack(meta, { replayed }));
  });
  const deliver = client(url), envelope = message();
  await assert.rejects(deliver(envelope, {}), isError('NETWORK'));
  assert.equal((await deliver(envelope, {})).replayed, true); assert.equal(writes, 1); assert.equal(attempts, 2);
});

// These helpers are deliberately narrow adapters around actual Node SQLite and
// HTTP; they do not replace any SDK codec, receiver, inbox or apply implementation.
function sqlTarget(db) {
  return { async transaction(work, options = {}) {
    const deadline = options.timeoutMs === undefined ? Infinity : performance.now() + options.timeoutMs;
    function check() { if (options.signal?.aborted || performance.now() >= deadline) throw Error('SQL scope cancelled'); }
    check(); db.exec('BEGIN');
    try {
      const result = await work({
        async execute(sql, params = []) { check(); return Number(db.prepare(sql).run(...params).changes); },
        async query(sql, params = []) {
          check(); const statement = db.prepare(sql); statement.setReadBigInts(true);
          return { rowArrays: statement.all(...params).map(row => Object.values(row)) };
        },
      });
      check(); db.exec('COMMIT'); return result;
    } catch (error) { db.exec('ROLLBACK'); throw error; }
  } };
}
function httpBridge(getHandler, afterResponse) {
  return async (q, r) => {
    const controller = new AbortController();
    const abort = () => { if (!r.writableFinished) controller.abort(new Error('HTTP peer disconnected')); };
    q.once('aborted', abort); r.once('close', abort);
    try {
      const init = { method: q.method, headers: q.headers, signal: controller.signal };
      if (q.method !== 'GET' && q.method !== 'HEAD') { init.body = Readable.toWeb(q); init.duplex = 'half'; }
      const response = await getHandler()(new Request(`http://${q.headers.host}${q.url}`, init));
      if (afterResponse?.(response, q, r)) return;
      r.writeHead(response.status, Object.fromEntries(response.headers));
      r.end(Buffer.from(await response.arrayBuffer()));
    } finally { q.off('aborted', abort); r.off('close', abort); }
  };
}
function wire(envelope = message(), edits = {}) {
  const metadata = { ...ack(envelope), ...edits };
  delete metadata.applied; delete metadata.omitted; delete metadata.confirmed; delete metadata.replayed;
  const json = Buffer.from(JSON.stringify(metadata));
  const bytes = Buffer.alloc(8 + json.length + envelope.changeset.length);
  bytes.set([70, 67, 68, 49]); bytes.writeUInt32BE(json.length, 4); bytes.set(json, 8); bytes.set(envelope.changeset, 8 + json.length);
  return bytes;
}
function request(body = wire(), options = {}) {
  return new Request('https://receiver.test/changes', { method: 'POST', body, duplex: 'half',
    ...options, headers: { 'content-type': REQUEST_TYPE, ...options.headers } });
}
const verifiedDouble = { receiverId: 'receiver', async receive(envelope) { return ack(envelope); } };
const deferred = () => { let resolve; const promise = new Promise(r => { resolve = r; }); return { promise, resolve }; };
const tick = () => new Promise(resolve => setImmediate(resolve));

test('endpoint construction requires explicit authorization and bounded policy', () => {
  for (const options of [{}, { authorize: true }, { authorize: () => true, maxInFlight: 0 },
    { authorize: () => true, maxMessageBytes: 64 * 1024 * 1024 + 1 }, { authorize: () => true, timeoutMs: NaN }]) {
    assert.throws(() => createChangesetHttpHandler(verifiedDouble, options), isError('INPUT', 'not-sent'));
  }
});
test('authorization runs before any body read and denial cancels the unread stream', async () => {
  let pulls = 0, cancelled = 0, receives = 0;
  const body = new ReadableStream({ pull() { pulls++; }, cancel() { cancelled++; } }, { highWaterMark: 0 });
  const handle = createChangesetHttpHandler({ receiverId: 'receiver', async receive() { receives++; } }, {
    authorize: async context => {
      assert.ok(Object.isFrozen(context)); assert.equal(context.receiverId, 'receiver');
      assert.equal(context.body, undefined); assert.equal(pulls, 0); return false;
    },
  });
  const response = await handle(request(body));
  assert.equal(response.status, 403); assert.equal(pulls, 0); assert.equal(receives, 0); assert.equal(cancelled, 1);
  assert.deepEqual(await response.json(), { error: 'ERR_FSQLITE_HTTP_REJECTED', outcome: 'unknown' });
});
for (const decision of [false, undefined, 'true', 1, {}, () => { throw Error('private authentication secret'); }]) {
  test(`authorization fails closed on ${typeof decision}`, async () => {
    let calls = 0;
    const handle = createChangesetHttpHandler({ receiverId: 'receiver', async receive() { calls++; } }, {
      authorize: typeof decision === 'function' ? decision : async () => decision,
    });
    const response = await handle(request()); assert.equal(response.status, 403);
    assert.equal(calls, 0); assert.equal((await response.text()).includes('secret'), false);
  });
}
test('delivery authorization can bind authenticated callers to a source namespace', async () => {
  let calls = 0;
  const handle = createChangesetHttpHandler({ receiverId: 'receiver', async receive(e) { calls++; return ack(e); } }, {
    authorize: ctx => ctx.headers.get('authorization') === 'Bearer sourceA',
    authorizeDelivery: (ctx, meta) => {
      assert.equal(meta.changeset, undefined); assert.ok(Object.isFrozen(meta));
      return ctx.headers.get('authorization') === 'Bearer sourceA' && meta.deliveryId.startsWith('sourceA:');
    },
  });
  assert.equal((await handle(request(wire(message(undefined, 'sourceB:1')), { headers: { authorization: 'Bearer sourceA' } }))).status, 403);
  assert.equal((await handle(request(wire(message(undefined, 'sourceA:1')), { headers: { authorization: 'Bearer sourceA' } }))).status, 200);
  assert.equal(calls, 1);
});
test('delivery authorization throw/false cannot leak errors or invoke receiver', async () => {
  for (const authorizeDelivery of [() => false, () => { throw Error('private policy'); }]) {
    let calls = 0;
    const handle = createChangesetHttpHandler({ receiverId: 'receiver', async receive() { calls++; } }, { authorize: () => true, authorizeDelivery });
    const response = await handle(request()); assert.equal(response.status, 403); assert.equal(calls, 0);
    assert.equal((await response.text()).includes('private'), false);
  }
});

const malformedFrames = [
  ['empty', () => new Uint8Array()],
  ['magic', () => { const bytes = wire(); bytes[0] = 0; return bytes; }],
  ['zero metadata', () => { const bytes = wire(); bytes.writeUInt32BE(0, 4); return bytes; }],
  ['oversized metadata', () => { const bytes = wire(); bytes.writeUInt32BE(8193, 4); return bytes; }],
  ['truncated metadata', () => wire().subarray(0, 12)],
  ['truncated payload', () => wire(message(new Uint8Array([1]))).subarray(0, -1)],
  ['trailing payload', () => Buffer.concat([wire(), Buffer.from([1])])],
  ['JSON', () => { const bytes = wire(); bytes[8] = 0; return bytes; }],
  ['UTF-8', () => { const bytes = wire(); bytes[8] = 255; return bytes; }],
  ['protocol', () => wire(message(), { protocol: 'old' })],
  ['receiver', () => wire(message(), { receiverId: 'elsewhere' })],
  ['identity', () => wire(message(), { deliveryId: '\0bad' })],
  ['digest', () => wire(message(), { sha256: 'g'.repeat(64) })],
  ['length', () => wire(message(), { byteLength: -1 })],
  ['declared payload cap', () => wire(message(), { byteLength: 64 * 1024 * 1024 + 1 })],
];
for (const [name, body] of malformedFrames) {
  test(`endpoint refuses malformed ${name} before SQL`, async () => {
    let calls = 0;
    const handle = createChangesetHttpHandler({ receiverId: 'receiver', async receive() { calls++; } }, { authorize: () => true });
    const response = await handle(request(body())); assert.equal(response.status, 400); assert.equal(calls, 0);
  });
}
test('chunked uploads obey actual-byte bounds and release the admission slot', async () => {
  let cancelled = 0, receives = 0, reads = 0;
  const body = new ReadableStream({ pull(controller) { reads++; controller.enqueue(new Uint8Array(4096)); }, cancel() { cancelled++; } }, { highWaterMark: 0 });
  const handle = createChangesetHttpHandler({ receiverId: 'receiver', async receive(e) { receives++; return ack(e); } }, { authorize: () => true, maxMessageBytes: 1 });
  assert.equal((await handle(request(body))).status, 413); assert.equal(receives, 0); assert.equal(cancelled, 1); assert.equal(reads, 3);
  assert.equal((await handle(request())).status, 200); assert.equal(receives, 1);
});
for (const headers of [{ 'content-encoding': 'gzip' }, { 'content-length': '-1' }, { 'content-length': '1' }, { 'content-length': '999999999' }]) {
  test(`endpoint rejects invalid wire headers ${JSON.stringify(headers)}`, async () => {
    let calls = 0;
    const handle = createChangesetHttpHandler({ receiverId: 'receiver', async receive() { calls++; } }, { authorize: () => true });
    assert.ok([400, 413].includes((await handle(request(wire(), { headers }))).status)); assert.equal(calls, 0);
  });
}
test('method/media gates run without entering authorization or SQL', async () => {
  let calls = 0;
  const handle = createChangesetHttpHandler(verifiedDouble, { authorize: () => { calls++; return true; } });
  const get = await handle(new Request('https://receiver.test/changes'));
  assert.equal(get.status, 405); assert.equal(get.headers.get('allow'), 'POST, OPTIONS');
  assert.equal((await handle(request(wire(), { headers: { 'content-type': 'application/json' } }))).status, 415);
  assert.equal(calls, 0);
});

test('CORS preflight uses explicit origins/headers and never grants SQL authority', async () => {
  let calls = 0;
  const handle = createChangesetHttpHandler(verifiedDouble, { authorize: () => { calls++; return false; },
    allowedOrigins: ['https://app.test'], allowedHeaders: ['x-api-key'] });
  const headers = { origin: 'https://app.test', 'access-control-request-method': 'POST', 'access-control-request-headers': 'content-type, Authorization, x-api-key' };
  const response = await handle(new Request('https://receiver.test/changes', { method: 'OPTIONS', headers }));
  assert.equal(response.status, 204); assert.equal(calls, 0);
  assert.equal(response.headers.get('access-control-allow-origin'), 'https://app.test');
  assert.equal(response.headers.get('access-control-allow-credentials'), null);
  assert.equal(response.headers.get('access-control-allow-methods'), 'POST');
  assert.equal(response.headers.get('access-control-allow-headers'), 'content-type, authorization, x-api-key');
  assert.equal(response.headers.get('cache-control'), 'no-store');
  const denied = await handle(request(wire(), { headers: { origin: 'https://app.test' } }));
  assert.equal(denied.status, 403); assert.equal(calls, 1);
  assert.equal(denied.headers.get('access-control-allow-origin'), 'https://app.test');
});
for (const origin of ['null', 'https://evil.test', 'https://app.test/', 'https://app.test, https://evil.test', 'https://user@app.test']) {
  test(`untrusted browser origin ${origin} rejects before auth`, async () => {
    let calls = 0;
    const handle = createChangesetHttpHandler(verifiedDouble, { authorize: () => { calls++; return true; }, allowedOrigins: ['https://app.test'] });
    const response = await handle(request(wire(), { headers: { origin } }));
    assert.equal(response.status, 403); assert.equal(calls, 0); assert.equal(response.headers.get('access-control-allow-origin'), null);
  });
}
test('same-origin and originless callers still require successful authorization', async () => {
  let calls = 0;
  const handle = createChangesetHttpHandler(verifiedDouble, { authorize: () => { calls++; return true; } });
  assert.equal((await handle(request())).status, 200);
  const same = await handle(request(wire(), { headers: { origin: 'https://receiver.test' } }));
  assert.equal(same.status, 200); assert.equal(calls, 2);
});
for (const options of [{ allowedOrigins: ['*'] }, { allowedOrigins: ['null'] }, { allowedOrigins: ['https://app.test/path'] },
  { allowedOrigins: ['https://user@app.test'] }, { allowedHeaders: ['*'] }, { allowedHeaders: ['Cookie'] }, { allowedHeaders: ['bad\r\nheader'] }]) {
  test(`invalid CORS configuration ${JSON.stringify(options)} fails at construction`, () => {
    assert.throws(() => createChangesetHttpHandler(verifiedDouble, { authorize: () => true, ...options }), isError('INPUT', 'not-sent'));
  });
}
test('unallowed preflight method/header and missing origin reject', async () => {
  const handle = createChangesetHttpHandler(verifiedDouble, { authorize: () => { throw Error('preflight must not authorize'); }, allowedOrigins: ['https://app.test'] });
  for (const headers of [{}, { origin: 'https://app.test', 'access-control-request-method': 'DELETE' },
    { origin: 'https://app.test', 'access-control-request-method': 'POST', 'access-control-request-headers': 'x-forbidden' }]) {
    assert.equal((await handle(new Request('https://receiver.test/changes', { method: 'OPTIONS', headers }))).status, 403);
  }
});

test('admission includes slow authorization and refuses excess requests without queueing', async () => {
  const entered = deferred(), release = deferred(); let auth = 0;
  const handle = createChangesetHttpHandler(verifiedDouble, { authorize: async () => {
    auth++; if (auth === 1) { entered.resolve(); await release.promise; } return true;
  } });
  const first = handle(request()); await entered.promise;
  assert.equal((await handle(request())).status, 503); assert.equal(auth, 1);
  release.resolve(); assert.equal((await first).status, 200);
  assert.equal((await handle(request())).status, 200); assert.equal(auth, 2);
});
test('cancellation retains the admission slot until receiver work actually drains', async () => {
  const entered = deferred(), release = deferred(), controller = new AbortController(); let calls = 0, finished = false, observed;
  const handle = createChangesetHttpHandler({ receiverId: 'receiver', async receive(e, controls) {
    calls++; if (calls === 1) { observed = controls.signal; entered.resolve(); await release.promise; } return ack(e);
  } }, { authorize: () => true });
  const first = handle(request(wire(), { signal: controller.signal })).then(result => { finished = true; return result; });
  await entered.promise; controller.abort(); await tick();
  assert.equal(observed.aborted, true); assert.equal(finished, false);
  assert.equal((await handle(request())).status, 503);
  release.resolve(); assert.equal((await first).status, 408);
  assert.equal((await handle(request())).status, 200); assert.equal(calls, 2);
});
test('stalled uploads time out, cancel the reader and release capacity', async () => {
  let cancelled = 0, calls = 0;
  const stream = new ReadableStream({ cancel() { cancelled++; } }, { highWaterMark: 0 });
  const handle = createChangesetHttpHandler({ receiverId: 'receiver', async receive(e) { calls++; return ack(e); } }, { authorize: () => true, timeoutMs: 30 });
  assert.equal((await handle(request(stream))).status, 408); assert.equal(cancelled, 1); assert.equal(calls, 0);
  assert.equal((await handle(request())).status, 200);
});
test('unconfirmed or mismatched receiver return values cannot become HTTP 200', async () => {
  for (const extra of [{ confirmed: false }, { receiverId: 'wrong' }, { omitted: -1 }]) {
    const handle = createChangesetHttpHandler({ receiverId: 'receiver', async receive(e) { return ack(e, extra); } }, { authorize: () => true });
    assert.equal((await handle(request())).status, 500);
  }
});
test('receiver SQL/confirmation errors are redacted rather than sent to the peer', async () => {
  const handle = createChangesetHttpHandler({ receiverId: 'receiver', async receive() { throw Error('SQL password secret\n stack trace'); } }, { authorize: () => true });
  const response = await handle(request()); assert.equal(response.status, 500);
  assert.deepEqual(await response.json(), { error: 'ERR_FSQLITE_HTTP_REJECTED', outcome: 'unknown' });
});

function nativeInsert(t, sql = "INSERT INTO notes VALUES(1,'initial')") {
  const db = new DatabaseSync(':memory:'); t.after(() => db.close());
  db.exec('CREATE TABLE notes(id INTEGER PRIMARY KEY, body TEXT)');
  const session = db.createSession(); db.exec(sql); const bytes = session.changeset(); session.close(); return bytes;
}
test('actual SDK receiver and inbox apply native session bytes over streaming HTTP', async t => {
  const destination = new DatabaseSync(':memory:'); t.after(() => destination.close());
  destination.exec('CREATE TABLE notes(id INTEGER PRIMARY KEY, body TEXT)');
  let confirmed = 0;
  const receiver = new ChangesetReceiver(sqlTarget(destination), { receiverId: 'receiver', tables: ['notes'], confirmCommit: async () => { confirmed++; } });
  const handle = createChangesetHttpHandler(receiver, { authorize: ctx => ctx.headers.get('authorization') === 'Bearer test' });
  const url = await server(t, httpBridge(() => handle));
  const deliver = client(url, { headers: () => ({ authorization: 'Bearer test' }) });
  const envelope = message(nativeInsert(t));
  const first = await deliver(envelope, {}); assert.equal(first.applied, 1); assert.equal(first.replayed, false);
  assert.equal((await deliver(envelope, {})).replayed, true);
  assert.equal(confirmed, 2); assert.equal(destination.prepare('SELECT count(*) AS n FROM notes').get().n, 1);
  assert.equal(destination.prepare('SELECT count(*) AS n FROM __fsqlite_changeset_receipts').get().n, 1);
});
test('lost HTTP acknowledgement after commit recovers from a reopened receiver file', async t => {
  const file = join(mkdtempSync(join(tmpdir(), 'fsqlite-http-')), 'receiver.db');
  let db = new DatabaseSync(file); t.after(() => db.close());
  db.exec('CREATE TABLE notes(id INTEGER PRIMARY KEY, body TEXT)');
  const makeHandler = () => createChangesetHttpHandler(new ChangesetReceiver(sqlTarget(db), {
    receiverId: 'receiver', tables: ['notes'], confirmCommit: async () => {},
  }), { authorize: () => true });
  let handle = makeHandler(), drop = true;
  const url = await server(t, httpBridge(() => handle, (response, _q, r) => {
    if (drop && response.status === 200) { drop = false; r.destroy(); return true; } return false;
  }));
  const deliver = client(url), envelope = message(nativeInsert(t));
  await assert.rejects(deliver(envelope, {}), isError('NETWORK'));
  db.close(); db = new DatabaseSync(file); handle = makeHandler();
  assert.equal((await deliver(envelope, {})).replayed, true);
  assert.equal(db.prepare('SELECT count(*) AS n FROM notes').get().n, 1);
  assert.equal(db.prepare('SELECT count(*) AS n FROM __fsqlite_changeset_receipts').get().n, 1);
});
test('failed receiver confirmation returns no ACK and a retry reconfirms its retained decision', async t => {
  const db = new DatabaseSync(':memory:'); t.after(() => db.close());
  db.exec('CREATE TABLE notes(id INTEGER PRIMARY KEY, body TEXT)'); let confirmations = 0;
  const receiver = new ChangesetReceiver(sqlTarget(db), { receiverId: 'receiver', tables: ['notes'], confirmCommit: async () => {
    if (++confirmations === 1) throw Error('private storage detail');
  } });
  const handle = createChangesetHttpHandler(receiver, { authorize: () => true });
  const url = await server(t, httpBridge(() => handle)), deliver = client(url), envelope = message(nativeInsert(t));
  await assert.rejects(deliver(envelope, {}), e => isError('HTTP')(e) && e.status === 500);
  assert.equal(db.prepare('SELECT count(*) AS n FROM notes').get().n, 1);
  assert.equal((await deliver(envelope, {})).replayed, true); assert.equal(confirmations, 2);
});
test('SQL constraint failure over HTTP rolls back earlier rows and the inbox', async t => {
  const db = new DatabaseSync(':memory:'); t.after(() => db.close());
  db.exec('CREATE TABLE notes(id INTEGER PRIMARY KEY CHECK(id<2), body TEXT)');
  let confirmations = 0;
  const handle = createChangesetHttpHandler(new ChangesetReceiver(sqlTarget(db), { receiverId: 'receiver', tables: ['notes'], confirmCommit: async () => { confirmations++; } }), { authorize: () => true });
  const url = await server(t, httpBridge(() => handle));
  await assert.rejects(client(url)(message(nativeInsert(t, "INSERT INTO notes VALUES(1,'a'),(2,'b')")), {}), isError('HTTP'));
  assert.equal(confirmations, 0); assert.equal(db.prepare('SELECT count(*) AS n FROM notes').get().n, 0);
  assert.equal(db.prepare("SELECT count(*) AS n FROM sqlite_schema WHERE name='__fsqlite_changeset_receipts'").get().n, 0);
});
test('digest tampering and unauthorized tables never reach receiver SQL', async t => {
  const db = new DatabaseSync(':memory:'); t.after(() => db.close());
  db.exec('CREATE TABLE notes(id INTEGER PRIMARY KEY, body TEXT)'); let transactions = 0;
  const target = sqlTarget(db), observed = { async transaction(...args) { transactions++; return target.transaction(...args); } };
  const handle = createChangesetHttpHandler(new ChangesetReceiver(observed, { receiverId: 'receiver', tables: [], confirmCommit: async () => {} }), { authorize: () => true });
  const url = await server(t, httpBridge(() => handle)), envelope = message(nativeInsert(t));
  await assert.rejects(client(url)({ ...envelope, sha256: '0'.repeat(64) }, {}), isError('HTTP'));
  await assert.rejects(client(url)(envelope, {}), isError('HTTP'));
  assert.equal(transactions, 0);
});

test('complete capture/outbox/pump/HTTP/receiver path preserves ordered work after a lost ACK', async t => {
  const source = new DatabaseSync(':memory:'), destination = new DatabaseSync(':memory:');
  t.after(() => { source.close(); destination.close(); });
  for (const db of [source, destination]) db.exec('CREATE TABLE notes(id INTEGER PRIMARY KEY, body TEXT); PRAGMA recursive_triggers=ON');
  const outbox = new ChangesetOutbox(sqlTarget(source)); let callbacks = 0;
  await outbox.record(async tx => { callbacks++; await tx.execute("INSERT INTO notes VALUES(1,'before')"); }, { tables: ['notes'], deliveryId: 'source:1' });
  await outbox.record(async tx => { callbacks++; await tx.execute("UPDATE notes SET body='after' WHERE id=1"); }, { tables: ['notes'], deliveryId: 'source:2' });
  const receiver = new ChangesetReceiver(sqlTarget(destination), { receiverId: 'receiver', tables: ['notes'], confirmCommit: async () => {} });
  const handle = createChangesetHttpHandler(receiver, { authorize: () => true }); let dropped = false;
  const url = await server(t, httpBridge(() => handle, (response, _q, r) => {
    if (response.status === 200 && !dropped) { dropped = true; r.destroy(); return true; } return false;
  }));
  const pump = new ChangesetDeliveryPump(outbox, { receiverId: 'receiver', confirmSource: async () => {}, deliver: client(url) });
  await assert.rejects(pump.run(), e => e.phase === 'transport' && e.cause instanceof ChangesetHttpError && e.cause.outcome === 'unknown');
  assert.equal((await outbox.pending()).length, 2);
  assert.equal(destination.prepare('SELECT body FROM notes').get().body, 'before');
  assert.equal((await outbox.record(() => { throw Error('callback replayed'); }, { tables: ['notes'], deliveryId: 'source:1' })).replayed, true);
  const recovered = await pump.run();
  assert.equal(recovered.deliveries, 2); assert.equal(recovered.replays, 1); assert.equal(recovered.stopped, 'empty');
  assert.equal(destination.prepare('SELECT body FROM notes').get().body, 'after');
  assert.deepEqual(await outbox.pending(), []); assert.equal(callbacks, 2);
  assert.equal(source.prepare('SELECT count(*) AS n FROM __fsqlite_changeset_outbox WHERE acknowledged=1 AND length(payload)=0').get().n, 2);
});

test('a separate HTTP receiver process killed after commit recovers its exact retained receipt', { timeout: 10_000 }, async t => {
  const path = join(mkdtempSync(join(tmpdir(), 'fsqlite-http-kill-')), 'receiver.db');
  const init = new DatabaseSync(path); init.exec('CREATE TABLE notes(id INTEGER PRIMARY KEY, body TEXT)'); init.close();
  const deliveryUrl = new URL('../src/changeset-delivery.ts', import.meta.url).href;
  const httpUrl = new URL('../src/changeset-http.ts', import.meta.url).href;
  const script = `
    import { DatabaseSync } from 'node:sqlite';
    import { createServer } from 'node:http';
    import { Readable } from 'node:stream';
    import { ChangesetReceiver } from ${JSON.stringify(deliveryUrl)};
    import { createChangesetHttpHandler } from ${JSON.stringify(httpUrl)};
    const sqlTarget = ${sqlTarget.toString()};
    const httpBridge = ${httpBridge.toString()};
    const db = new DatabaseSync(${JSON.stringify(path)});
    const receiver = new ChangesetReceiver(sqlTarget(db), { receiverId:'receiver', tables:['notes'],
      confirmCommit: async () => { process.kill(process.pid, 'SIGKILL'); } });
    const handler = createChangesetHttpHandler(receiver, { authorize: () => true });
    const bridge = httpBridge(() => handler);
    const server = createServer((q,r) => { bridge(q,r).catch(() => r.destroy()); });
    server.listen(0,'127.0.0.1', () => process.send({ url: 'http://127.0.0.1:' + server.address().port + '/changes' }));
  `;
  const loaderFlags = process.execArgv.filter(a => a.startsWith('--experimental-loader=') || a.startsWith('--loader='));
  assert.ok(loaderFlags.length, 'run with the documented TypeScript source loader');
  const child = spawn(process.execPath, [...loaderFlags, '--input-type=module', '-e', script], { stdio: ['ignore', 'ignore', 'pipe', 'ipc'] });
  let diagnostics = ''; child.stderr.on('data', bytes => { diagnostics = (diagnostics + bytes).slice(-8192); });
  const exited = once(child, 'exit');
  t.after(() => { if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL'); });
  const startup = await Promise.race([
    once(child, 'message').then(([value]) => value),
    exited.then(() => { throw Error(`Receiver exited before startup: ${diagnostics}`); }),
  ]);
  const envelope = message(nativeInsert(t));
  await assert.rejects(client(startup.url)(envelope, {}), isError('NETWORK'));
  const [, signal] = await exited; assert.equal(signal, 'SIGKILL');
  const reopened = new DatabaseSync(path); t.after(() => reopened.close());
  assert.equal(reopened.prepare('SELECT count(*) AS n FROM notes').get().n, 1);
  const handler = createChangesetHttpHandler(new ChangesetReceiver(sqlTarget(reopened), {
    receiverId: 'receiver', tables: ['notes'], confirmCommit: async () => {},
  }), { authorize: () => true });
  const newUrl = await server(t, httpBridge(() => handler));
  const result = await client(newUrl)(envelope, {});
  assert.equal(result.replayed, true); assert.equal(result.applied, 1);
  assert.equal(reopened.prepare('SELECT count(*) AS n FROM notes').get().n, 1);
  assert.equal(reopened.prepare('SELECT count(*) AS n FROM __fsqlite_changeset_receipts').get().n, 1);
});
