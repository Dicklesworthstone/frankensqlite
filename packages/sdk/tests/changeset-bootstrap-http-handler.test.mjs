import assert from 'node:assert/strict';
import { test } from 'node:test';
import { createServer } from 'node:http';
import { Readable } from 'node:stream';
import { once } from 'node:events';
import { fork } from 'node:child_process';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { ChangesetBootstrapTransfer } from '../src/changeset-bootstrap-transfer.ts';
import { Source, Receiver } from './helpers/bootstrap-transfer-fixture.mjs';
import {
  createBootstrapHttpTransport, createBootstrapHttpHandler, BOOTSTRAP_HTTP_PROTOCOL,
  BOOTSTRAP_HTTP_CONTENT_TYPE, BOOTSTRAP_HTTP_ACTION_HEADER,
} from '../src/changeset-bootstrap-http.ts';

// This suite executes production HTTP+transfer code. Source/store/receiver are
// the explicitly named SQLite-backed boundary fixtures from the transfer suite.
// It does not certify production bootstrap/store/fanout or the Rust/WASM engine.
const good = { protocol: 'fsqlite-bootstrap-v1', receiverId: 'east', deliveryId: 'source:seed', tables: ['t'], chunks: 1, changes: 0, byteLength: 0, sha256: 'a'.repeat(64) };
const full = (m, installed = false) => ({ receivedChunks: m.chunks, receivedBytes: m.byteLength, receivedChanges: m.changes, installed });
const receipt = (m, order) => ({ protocol: m.protocol, receiverId: m.receiverId, deliveryId: m.deliveryId, sha256: m.sha256,
  chunks: m.chunks, changes: m.changes, byteLength: m.byteLength, installed: true, confirmed: true, replayed: false, ...(order ? { order } : {}) });
const wait = ms => new Promise(resolve => setTimeout(resolve, ms));
function gate() { let resolve; const promise = new Promise(r => { resolve = r; }); return { promise, resolve }; }
function frame(action = 'status', m = good, bytes = new Uint8Array(), extra = {}) {
  const info = { protocol: BOOTSTRAP_HTTP_PROTOCOL, action, manifest: m, byteLength: bytes.length,
    ...(action === 'stage' ? { index: 0 } : {}), ...extra };
  const json = Buffer.from(JSON.stringify(info)), wire = Buffer.alloc(8 + json.length + bytes.length);
  wire.write('FCB1'); wire.writeUInt32BE(json.length, 4); wire.set(json, 8); wire.set(bytes, 8 + json.length); return wire;
}
function request(action = 'status', options = {}) {
  const { bytes = frame(action), headers, ...rest } = options;
  return new Request('https://replica.example/bootstrap', { method: 'POST', body: bytes,
    headers: { 'content-type': BOOTSTRAP_HTTP_CONTENT_TYPE, [BOOTSTRAP_HTTP_ACTION_HEADER]: action, ...headers }, ...rest });
}
function fake(overrides = {}) {
  return { receiverId: 'east', status: async () => null, stage: async m => full(m), install: async m => receipt(m), ...overrides };
}
const handler = (receiver = fake(), options = {}) => createBootstrapHttpHandler(receiver, { authorize: () => true, ...options });
async function server(t, handle, after) {
  const s = createServer(async (req, res) => {
    const c = new AbortController(); req.on('aborted', () => c.abort()); res.on('close', () => { if (!res.writableEnded) c.abort(); });
    try {
      const r = new Request(`http://127.0.0.1:${s.address().port}${req.url}`, { method: req.method, headers: req.headers,
        body: req.method === 'GET' || req.method === 'HEAD' ? undefined : Readable.toWeb(req), duplex: 'half', signal: c.signal });
      const response = await handle(r); if (await after?.(r, response, res)) return;
      if (!res.destroyed) { res.writeHead(response.status, Object.fromEntries(response.headers)); res.end(Buffer.from(await response.arrayBuffer())); }
    } catch { if (!res.destroyed) res.destroy(); }
  });
  s.listen(0, '127.0.0.1'); await once(s, 'listening');
  t.after(() => new Promise(resolve => { s.closeAllConnections(); s.close(resolve); }));
  return `http://127.0.0.1:${s.address().port}/bootstrap`;
}
function setup(t, count = 3, orderedSourceId) {
  const source = new Source({ count }); const receiver = new Receiver(source, { orderedSourceId }); receiver.receiverId = 'east';
  t.after(() => { source.close(); receiver.close(); }); return { source, receiver };
}
const route = { receiverId: 'east', deliveryId: 'source:seed', tables: ['t'] };
const remote = (url, options = {}) => createBootstrapHttpTransport(url, { allowInsecureLoopback: true,
  headers: () => ({ authorization: 'Bearer test' }), ...options });
const transfer = (source, transport, options = {}) => new ChangesetBootstrapTransfer(source, {
  ...route, transport, confirmSource: () => source.confirm(), ...options,
});

test('real HTTP coordinator resumes a prefix and only reclaims after complete install', async t => {
  const { source, receiver } = setup(t, 12);
  let confirmations = 0; receiver.hooks.confirm = () => { confirmations++; assert.equal(source.pending(), 12); };
  const address = await server(t, handler(receiver, { authorize: c => c.headers.get('authorization') === 'Bearer test' }));
  const driver = transfer(source, remote(address));
  const first = await driver.run({ maxChunks: 5 });
  assert.equal(first.stopped, 'limit'); assert.equal(first.receivedChunks, 5); assert.equal(source.pending(), 12); assert.deepEqual(receiver.rows(), []);
  const finished = await driver.run(); assert.equal(finished.stopped, 'installed'); assert.equal(finished.uploadedChunks, 7);
  assert.equal(source.pending(), 0); assert.equal(receiver.rows().length, 12); assert.equal(confirmations, 1);
  receiver.hooks.confirm = () => { confirmations++; };
  receiver.db.exec("UPDATE t SET value='later' WHERE id=1");
  const replay = await transfer(source, remote(address)).run(); assert.equal(replay.receipt.replayed, true);
  assert.equal(replay.uploadedChunks, 0); assert.equal(confirmations, 2); assert.equal(receiver.rows()[0].value, 'later');
});
test('a lost successful install response leaves source pending and replay confirms again', async t => {
  const { source, receiver } = setup(t); let dropped = false, confirms = 0;
  receiver.hooks.confirm = () => { confirms++; };
  const address = await server(t, handler(receiver), (req, response, res) => {
    if (!dropped && req.headers.get(BOOTSTRAP_HTTP_ACTION_HEADER) === 'install' && response.status === 200) { dropped = true; res.destroy(); return true; }
  });
  await assert.rejects(transfer(source, remote(address)).run()); assert.equal(source.pending(), 3); assert.equal(receiver.rows().length, 3);
  const r = await transfer(source, remote(address)).run(); assert.equal(r.receipt.replayed, true); assert.equal(r.uploadedChunks, 0);
  assert.equal(confirms, 2); assert.equal(source.pending(), 0);
});
test('failed receiver confirmation and lost source ACK preserve recovery identity over HTTP', async t => {
  const { source, receiver } = setup(t); let confirms = 0;
  receiver.hooks.confirm = () => { if (++confirms === 1) throw Error('private checkpoint failure'); };
  const address = await server(t, handler(receiver)); const driver = transfer(source, remote(address));
  await assert.rejects(driver.run()); assert.equal(source.pending(), 3); assert.equal(receiver.rows().length, 3);
  source.hooks.afterAck = () => { throw Error('lost source ACK'); };
  await assert.rejects(driver.run()); assert.equal(source.pending(), 0);
  source.hooks.afterAck = undefined; const r = await driver.run();
  assert.equal(r.receipt.replayed, true); assert.equal(r.newlyAcknowledged, 0); assert.equal(confirms, 3);
});
test('ordered source binding survives HTTP and fanout retains slow replica payloads', async t => {
  const { source, receiver } = setup(t, 3, 'source:incarnation');
  const address = await server(t, handler(receiver, { orderedSourceId: 'source:incarnation' }));
  const options = { orderedSourceId: 'source:incarnation', acknowledgement: 'fanout' };
  const r = await transfer(source, remote(address, options), options).run();
  assert.equal(r.receipt.order.streamId, 'source:incarnation'); assert.equal(r.receipt.order.sequence, '3');
  assert.equal(source.pending(), 3, 'west is still pending');
  assert.equal(source.db.prepare("SELECT head FROM replica WHERE id='east'").get().head, 3);
});
test('late native SQL constraint error installs no application prefix', async t => {
  const { source, receiver } = setup(t); receiver.db.exec("DROP TABLE t; CREATE TABLE t(id INTEGER PRIMARY KEY, value CHECK(value <> 'row-2'))");
  const address = await server(t, handler(receiver));
  await assert.rejects(transfer(source, remote(address)).run());
  assert.equal(source.pending(), 3); assert.deepEqual(receiver.rows(), []); assert.equal(receiver.db.prepare('SELECT count(*) n FROM chunks').get().n, 3);
});
test('empty baseline still stages and confirms one empty chunk', async t => {
  const { source, receiver } = setup(t, 0); const address = await server(t, handler(receiver));
  const r = await transfer(source, remote(address)).run(); assert.equal(r.manifest.chunks, 1); assert.equal(r.manifest.changes, 0);
  assert.equal(r.installed, true); assert.deepEqual(receiver.rows(), []); assert.equal(source.pending(), 0);
});
for (const approval of [false, undefined, 1, 'true', {}, 'throw']) test(`authorization fails closed for ${String(approval)}`, async () => {
  let reads = 0, cancels = 0, sql = 0;
  const stream = new ReadableStream({ pull(c) { reads++; c.enqueue(new Uint8Array(1)); }, cancel() { cancels++; } }, { highWaterMark: 0 });
  const h = handler(fake({ status: async () => { sql++; } }), { authorize: () => { if (approval === 'throw') throw Error('secret'); return approval; } });
  const r = await h(request('status', { bytes: stream, duplex: 'half' }));
  assert.equal(r.status, 403); assert.equal(reads, 0); assert.equal(cancels, 1); assert.equal(sql, 0); assert.ok(!(await r.text()).includes('secret'));
});
test('manifest authorization sees a detached scope and no body before SQL', async () => {
  let called = 0; const h = handler(fake({ stage: async () => { called++; } }), {
    authorize: c => { assert.equal(c.action, 'stage'); assert.equal('body' in c, false); return true; },
    authorizeManifest: (c, info) => { assert.equal(info.manifest.receiverId, c.receiverId); assert.ok(Object.isFrozen(info.manifest.tables)); assert.equal('bytes' in info, false); return false; },
  });
  assert.equal((await h(request('stage'))).status, 403); assert.equal(called, 0);
});
test('unauthorized status and install cannot bypass policy by omitting Origin', async () => {
  const h = handler(fake(), { authorize: () => false });
  for (const op of ['status', 'install']) assert.equal((await h(request(op))).status, 403);
});
test('CORS preflight permits exact origins and the action header, never credentials', async () => {
  let auth = 0; const h = handler(fake(), { authorize: () => { auth++; return true; }, allowedOrigins: ['https://app.example'] });
  const r = await h(new Request('https://replica.example/bootstrap', { method: 'OPTIONS', headers: {
    origin: 'https://app.example', 'access-control-request-method': 'POST', 'access-control-request-headers': `authorization,content-type,${BOOTSTRAP_HTTP_ACTION_HEADER}`,
  } }));
  assert.equal(r.status, 204); assert.equal(auth, 0); assert.equal(r.headers.get('access-control-allow-origin'), 'https://app.example');
  assert.ok(r.headers.get('access-control-allow-headers').includes(BOOTSTRAP_HTTP_ACTION_HEADER)); assert.equal(r.headers.has('access-control-allow-credentials'), false);
});
for (const origin of ['null', '*', 'https://evil.example', 'https://app.example/path', 'https://user@app.example']) test(`CORS refuses ${origin}`, async () => {
  let sql = 0; const h = handler(fake({ status: async () => { sql++; } }), { allowedOrigins: ['https://app.example'] });
  const r = await h(request('status', { headers: { origin } })); assert.equal(r.status, 403); assert.equal(sql, 0); assert.equal(r.headers.has('access-control-allow-origin'), false);
});
test('same-origin requests still authenticate and receive no-store/nosniff/Vary', async () => {
  let called = 0; const r = await handler(fake(), { authorize: () => { called++; return true; } })(request('status', { headers: { origin: 'https://replica.example' } }));
  assert.equal(r.status, 200); assert.equal(called, 1); assert.equal(r.headers.get('cache-control'), 'no-store');
  assert.equal(r.headers.get('x-content-type-options'), 'nosniff'); assert.ok(r.headers.get('vary').includes('Origin'));
});
for (const names of ['cookie', 'x-unlisted', 'authorization,', '*']) test(`preflight refuses ${names}`, async () => {
  const r = await handler()(new Request('https://replica.example/bootstrap', { method: 'OPTIONS', headers: { origin: 'https://replica.example', 'access-control-request-method': 'POST', 'access-control-request-headers': names } })); assert.equal(r.status, 403);
});
for (const options of [{ authorize: null }, { authorizeManifest: 1 }, { maxInFlight: 0 }, { maxInFlight: 65 }, { maxChunkBytes: 0 }, { timeoutMs: 0 }, { allowedOrigins: ['*'] }, { allowedOrigins: ['https://app.example/'] }, { allowedHeaders: ['cookie'] }, { allowedHeaders: ['*'] }]) {
  test(`handler rejects configuration ${JSON.stringify(options)}`, () => assert.throws(() => handler(fake(), options)));
}
for (const op of [null, '', 'discard', 'begin', 'STAGE']) test(`unsupported action ${op} cannot call a receiver`, async () => {
  const r = request(); if (op === null) r.headers.delete(BOOTSTRAP_HTTP_ACTION_HEADER); else r.headers.set(BOOTSTRAP_HTTP_ACTION_HEADER, op);
  assert.equal((await handler()(r)).status, 400);
});
test('method/media errors precede body reading and never accept ordinary changeset framing', async () => {
  const h = handler(); assert.equal((await h(new Request('https://replica.example/bootstrap'))).status, 405);
  assert.equal((await h(request('status', { headers: { 'content-type': 'application/vnd.fsqlite.changeset.v1' } }))).status, 415);
  const bytes = frame(); bytes[2] = 68; assert.equal((await h(request('status', { bytes }))).status, 400);
});
for (const [label, bytes] of [
  ['truncated', new Uint8Array(3)], ['tail', Buffer.concat([frame(), Buffer.from([1])])],
  ['action mismatch', frame('install')], ['protocol', frame('status', good, new Uint8Array(), { protocol: 'other' })],
  ['wrong route', frame('status', { ...good, receiverId: 'west' })], ['control index', frame('status', good, new Uint8Array(), { index: 0 })],
  ['negative chunk index', frame('stage', good, new Uint8Array(), { index: -1 })],
]) test(`invalid frame ${label} never enters receiver`, async () => {
  let calls = 0; const r = await handler(fake({ status: async () => { calls++; }, stage: async () => { calls++; } }))(request('status', { bytes }));
  assert.equal(r.status, 400); assert.equal(calls, 0);
});
test('control body and declared upload bounds cancel without entering SQL', async () => {
  let calls = 0, pulls = 0, cancelled = 0;
  const h = handler(fake({ status: async () => { calls++; } }), { maxChunkBytes: 1 });
  const stream = new ReadableStream({ pull(c) { pulls++; c.enqueue(new Uint8Array(65536)); }, cancel() { cancelled++; } }, { highWaterMark: 0 });
  assert.equal((await h(request('status', { bytes: stream, duplex: 'half' }))).status, 413);
  assert.equal(calls, 0); assert.equal(cancelled, 1); assert.ok(pulls <= 3);
  assert.equal((await h(request('stage', { headers: { 'content-length': '999999999' } }))).status, 413);
});
for (const [label, headers] of [['compressed', { 'content-encoding': 'gzip' }], ['malformed length', { 'content-length': '-1' }], ['short length', { 'content-length': '1' }], ['truncated', { 'content-length': '10000' }]]) {
  test(`handler rejects ${label}`, async () => assert.ok([400, 413].includes((await handler()(request('status', { headers }))).status)));
}
test('admission counts pending authorization; overflow never queues or reads a body', async () => {
  const auth = gate(), entered = gate(); let count = 0, pulls = 0;
  const h = handler(fake(), { authorize: async () => { if (++count === 1) { entered.resolve(); await auth.promise; } return true; } });
  const first = h(request()); await entered.promise;
  const stream = new ReadableStream({ pull(c) { pulls++; c.close(); } }, { highWaterMark: 0 });
  assert.equal((await h(request('status', { bytes: stream, duplex: 'half' }))).status, 503); assert.equal(pulls, 0); assert.equal(count, 1);
  auth.resolve(); assert.equal((await first).status, 200); assert.equal((await h(request())).status, 200);
});
test('cancelled installation holds admission through receiver confirmation', async () => {
  const completed = gate(), entered = gate(); const c = new AbortController();
  const h = handler(fake({ install: async m => { entered.resolve(); await completed.promise; return receipt(m); } }));
  let settled = false; const first = h(request('install', { signal: c.signal })).then(r => { settled = true; return r; });
  await entered.promise; c.abort(); await wait(5); assert.equal(settled, false);
  assert.equal((await h(request())).status, 503); completed.resolve(); assert.equal((await first).status, 408);
  assert.equal((await h(request())).status, 200);
});
test('aborted upload holds admission while body cancellation cleanup drains', async () => {
  const cleaning = gate(), entered = gate(); const c = new AbortController();
  const stream = new ReadableStream({ pull() { entered.resolve(); }, cancel() { return cleaning.promise; } }, { highWaterMark: 0 });
  const h = handler(); let settled = false;
  const pending = h(request('stage', { bytes: stream, duplex: 'half', signal: c.signal })).then(r => { settled = true; return r; });
  await entered.promise; c.abort(); await wait(5); assert.equal(settled, false); assert.equal((await h(request())).status, 503);
  cleaning.resolve(); assert.equal((await pending).status, 408); assert.equal(stream.locked, false);
});
test('handler deadline reaches receiver and never returns an installation ACK after expiry', async () => {
  const h = handler(fake({ install: async (m, controls) => { await new Promise(resolve => controls.signal.addEventListener('abort', resolve, { once: true })); return receipt(m); } }), { timeoutMs: 15 });
  const r = await h(request('install')); assert.equal(r.status, 408); assert.equal((await h(request())).status, 200);
});
for (const [suffix, status] of [['BUSY', 503], ['STATE', 409], ['INPUT', 400], ['LIMIT', 413], ['CANCELLED', 408], ['TIMEOUT', 408], ['CORRUPT', 500], ['CONFIRM', 500]]) {
  test(`receiver ${suffix} error is redacted and does not claim rollback`, async () => {
    const h = handler(fake({ install: async () => { const e = Error('sensitive database contents'); e.code = `ERR_FSQLITE_BOOTSTRAP_${suffix}`; throw e; } }));
    const r = await h(request('install')); assert.equal(r.status, status); const body = await r.json();
    assert.equal(body.outcome, 'unknown'); assert.ok(!JSON.stringify(body).includes('sensitive'));
  });
}
for (const [label, result] of [['status is not receipt', { ...full(good), installed: true }], ['unconfirmed', { ...receipt(good), confirmed: false }], ['wrong identity', { ...receipt(good), deliveryId: 'other' }], ['wrong counts', { ...receipt(good), chunks: 2 }]]) {
  test(`handler refuses receiver ${label}`, async () => assert.equal((await handler(fake({ install: async () => result }))(request('install'))).status, 500));
}
test('handler ordered policy rejects stripped or wrong source receipts', async () => {
  for (const order of [undefined, { protocol: 'fsqlite-ordered-changeset-v1', streamId: 'other', sequence: '1' }]) {
    assert.equal((await handler(fake({ install: async m => receipt(m, order) }), { orderedSourceId: 'source:1' })(request('install'))).status, 500);
  }
});
async function child(t, filename, cut, reopen) {
  const process = fork(new URL('./helpers/bootstrap-http-child.mjs', import.meta.url), [], { execArgv: ['--experimental-transform-types'],
    env: { ...globalThis.process.env, BOOTSTRAP_HTTP_DB: filename, BOOTSTRAP_HTTP_CUT: cut, BOOTSTRAP_HTTP_REOPEN: reopen ? '1' : '0' }, silent: true });
  let errors = ''; process.stderr.on('data', data => { errors += data; });
  t.after(() => { if (process.exitCode === null && process.signalCode === null) process.kill('SIGTERM'); });
  const exit = once(process, 'exit');
  const message = await Promise.race([once(process, 'message'), exit.then(() => { throw Error(`child exited before listen: ${errors}`); })]);
  return { process, exit, url: `http://127.0.0.1:${message[0].port}/bootstrap` };
}
for (const cut of ['stage', 'row', 'commit']) test(`real HTTP process SIGKILL at ${cut} recovers retained seed and atomic native SQL`, async t => {
  const directory = mkdtempSync(join(tmpdir(), 'fsqlite-http-cut-')), filename = join(directory, 'receiver.db');
  const source = new Source(); t.after(() => source.close());
  const first = await child(t, filename, cut, false);
  await assert.rejects(transfer(source, remote(first.url)).run()); const [, signal] = await first.exit; assert.equal(signal, 'SIGKILL');
  assert.equal(source.pending(), 3);
  const probe = new DatabaseSync(filename);
  assert.equal(probe.prepare('SELECT count(*) n FROM t').get().n, cut === 'commit' ? 3 : 0); probe.close();
  const second = await child(t, filename, '', true);
  const result = await transfer(source, remote(second.url)).run(); assert.equal(result.installed, true); assert.equal(source.pending(), 0);
  assert.equal(result.receipt.replayed, cut === 'commit');
  const final = new DatabaseSync(filename); assert.equal(final.prepare('SELECT count(*) n FROM t').get().n, 3); final.close();
});
for (const index of [-1, 1, 0.5, null]) test(`stage framing rejects actual index ${index}`, async () => {
  let called = 0;
  const r = await handler(fake({ stage: async () => { called++; } }))(request('stage', { bytes: frame('stage', good, new Uint8Array(), { index }) }));
  assert.equal(r.status, 400); assert.equal(called, 0);
});
for (const length of [0, 131073, 0xffffffff]) test(`frame metadata length ${length} rejects`, async () => {
  const bytes = frame(); bytes.writeUInt32BE(length, 4);
  assert.equal((await handler()(request('status', { bytes }))).status, 400);
});
test('invalid UTF-8 metadata rejects before receiver', async () => {
  const bytes = frame(); bytes[8] = 0xff;
  assert.equal((await handler()(request('status', { bytes }))).status, 400);
});
test('handler captures private-field receiver methods with correct ownership', async () => {
  class Owned {
    receiverId = 'east'; #calls = 0;
    async status() { this.#calls++; return null; }
    async stage(m) { this.#calls++; return full(m); }
    async install(m) { this.#calls++; return receipt(m); }
    get count() { return this.#calls; }
  }
  const receiver = new Owned(), h = handler(receiver);
  receiver.status = async () => { throw Error('changed method'); };
  for (const op of ['status', 'stage', 'install']) assert.equal((await h(request(op))).status, 200);
  assert.equal(receiver.count, 3);
});
