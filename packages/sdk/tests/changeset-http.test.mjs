import assert from 'node:assert/strict';
import { test } from 'node:test';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { createHash } from 'node:crypto';
import { DatabaseSync } from 'node:sqlite';
import {
  createChangesetHttpTransport, ChangesetHttpError,
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
