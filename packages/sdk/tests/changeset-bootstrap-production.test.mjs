import assert from 'node:assert/strict';
import { test } from 'node:test';
import { ChangesetOutbox } from '../src/changeset-outbox.ts';
import { ChangesetBootstrapTransfer } from '../src/changeset-bootstrap-transfer.ts';
import { ChangesetBootstrapReceiver, readBootstrapManifest } from '../src/changeset-bootstrap.ts';
import { ChangesetFanout } from '../src/changeset-fanout.ts';
import { ChangesetOrder } from '../src/changeset-order.ts';
import { applyChangeset } from '../src/changeset-apply.ts';
import { SqliteTarget } from './helpers/production-sqlite-target.mjs';

const schema = 'CREATE TABLE t(id INTEGER PRIMARY KEY, value TEXT);';
const root = 'source:seed', sourceId = 'source:epoch';
async function sourceWithSeed(n = 12, replicas) {
  const source = new SqliteTarget(':memory:', schema);
  if (replicas) await ChangesetFanout.open(source, replicas);
  for (let i = 0; i < n; i++) await source.execute('INSERT INTO t VALUES (?, ?)', [BigInt(i + 1), `row-${i}`]);
  const outbox = new ChangesetOutbox(source);
  const seed = await outbox.bootstrapChunks({ deliveryId: root, tables: ['t'], chunkRows: 1 });
  return { source, outbox, seed };
}
function connection(source, destination, options = {}) {
  const receiverId = options.receiverId ?? 'replica';
  const receiver = new ChangesetBootstrapReceiver(destination, {
    receiverId, tables: ['t'], ...options.receiver,
    ...(options.ordered ? { orderedSourceId: sourceId } : {}),
    confirmCommit: options.confirmDestination ?? (async () => { assert.equal(destination.active, false); }),
  });
  const transfer = new ChangesetBootstrapTransfer(source, {
    receiverId, deliveryId: root, tables: ['t'], transport: options.transport?.(receiver) ?? receiver,
    ...(options.ordered ? { orderedSourceId: sourceId } : {}),
    ...(options.fanout ? { acknowledgement: 'fanout' } : {}),
    confirmSource: options.confirmSource ?? (async () => { assert.equal(source.active, false); }),
  });
  return { receiver, transfer };
}
function pendingBytes(source) {
  return source.rows('SELECT sum(length(payload)) FROM __fsqlite_changeset_outbox')[0][0];
}

for (const ordered of [false, true]) {
  test(`production ${ordered ? 'ordered' : 'unordered'} capture/outbox/receiver/transfer resumes without fixture substitution`, async () => {
    const { source, outbox, seed } = await sourceWithSeed();
    const destination = new SqliteTarget(':memory:', schema);
    const { receiver, transfer } = connection(source, destination, { ordered });
    try {
      assert.equal(seed.chunks, 12);
      const first = await transfer.run({ maxChunks: 3 });
      assert.equal(first.stopped, 'limit'); assert.equal(first.uploadedChunks, 3);
      assert.deepEqual(destination.rows(), []); assert.equal((await outbox.pending()).length, 12);
      const original = pendingBytes(source);
      assert.ok(original > 0n);
      let callbacks = 0;
      const increment = await outbox.record(async tx => {
        callbacks++;
        await tx.execute("UPDATE t SET value='newer' WHERE id=1");
        await tx.execute("INSERT INTO t VALUES(100,'after seed')");
      }, { deliveryId: 'source:next', tables: ['t'] });
      const completed = await transfer.run();
      assert.equal(completed.stopped, 'installed'); assert.equal(completed.uploadedChunks, 9);
      assert.equal(completed.newlyAcknowledged, 12);
      assert.equal(destination.rows().length, 12); assert.equal(destination.rows()[0][1], 'row-0');
      assert.equal((await outbox.pending()).length, 1);
      const message = await outbox.read(increment.delivery.deliveryId);
      assert.ok(message?.changeset);
      if (ordered) {
        const ledger = new ChangesetOrder(destination, { receiverId: 'replica', sourceId });
        assert.equal((await ledger.head()).sequence, 12n);
        await ledger.apply({ sequence: message.delivery.sequence, deliveryId: message.delivery.deliveryId,
          sha256: message.delivery.sha256, changeset: message.changeset },
          (target, bytes) => applyChangeset(target, bytes, { tables: ['t'] }));
        assert.equal((await ledger.head()).sequence, 13n);
      } else await applyChangeset(destination, message.changeset, { tables: ['t'], deliveryId: message.delivery.deliveryId });
      await outbox.acknowledge(message.delivery.deliveryId, message.delivery.sha256);
      assert.deepEqual(destination.rows(), source.rows());
      const replay = await transfer.run();
      assert.equal(replay.receipt.replayed, true); assert.equal(replay.uploadedChunks, 0);
      assert.equal(replay.newlyAcknowledged, 0); assert.equal(callbacks, 1);
      assert.deepEqual(destination.rows(), source.rows());
      assert.equal((await receiver.status(first.manifest)).installed, true);
    } finally { source.close(); destination.close(); }
  });
}

test('production fanout retains the entire seed until the slow replica confirms installation', async () => {
  const { source, outbox } = await sourceWithSeed(4, ['east', 'west']);
  const east = new SqliteTarget(':memory:', schema), west = new SqliteTarget(':memory:', schema);
  try {
    const fanout = await ChangesetFanout.open(source, ['east', 'west']);
    const a = connection(source, east, { receiverId: 'east', ordered: true, fanout: true });
    const b = connection(source, west, { receiverId: 'west', ordered: true, fanout: true });
    const retained = pendingBytes(source);
    const first = await a.transfer.run();
    assert.equal(first.newlyAcknowledged, 4); assert.equal(pendingBytes(source), retained);
    assert.equal((await fanout.progress()).acknowledgedThrough, 0n);
    await b.transfer.run({ maxChunks: 2 });
    assert.deepEqual(west.rows(), []); assert.equal(pendingBytes(source), retained);
    assert.equal((await a.transfer.run()).newlyAcknowledged, 0);
    const last = await b.transfer.run();
    assert.equal(last.newlyAcknowledged, 4); assert.equal(pendingBytes(source), 0n);
    assert.deepEqual(east.rows(), source.rows()); assert.deepEqual(west.rows(), source.rows());
    assert.equal((await fanout.progress()).acknowledgedThrough, 4n);
    assert.deepEqual(await outbox.pending(), []);
  } finally { source.close(); east.close(); west.close(); }
});

test('lost production install ACK plus damaged unordered replay cannot reclaim source bytes', async () => {
  const { source, outbox } = await sourceWithSeed(3);
  const destination = new SqliteTarget(':memory:', schema);
  let lose = true;
  const { transfer } = connection(source, destination, { transport: receiver => ({
    status: receiver.status.bind(receiver), stage: receiver.stage.bind(receiver),
    install: async (...args) => {
      const result = await receiver.install(...args);
      if (lose) { lose = false; throw new Error('lost ACK'); }
      return result;
    },
  }) });
  try {
    const before = pendingBytes(source);
    await assert.rejects(transfer.run(), error => error.phase === 'install');
    assert.deepEqual(destination.rows(), source.rows());
    assert.equal(pendingBytes(source), before);
    destination.db.exec("UPDATE __fsqlite_bootstrap_chunks SET sha256=printf('%064d',0) WHERE idx=0");
    await assert.rejects(transfer.run(), error => error.phase === 'install' && error.cause?.code === 'ERR_FSQLITE_BOOTSTRAP_CORRUPT');
    assert.equal(pendingBytes(source), before); assert.equal((await outbox.pending()).length, 3);
    // Manifest recovery remains possible at the source; never invent another seed.
    const manifest = await readBootstrapManifest(source, { receiverId: 'replica', deliveryId: root, tables: ['t'] });
    assert.equal(manifest.chunks, 3);
  } finally { source.close(); destination.close(); }
});

// These imports are resolved to the production modules by the resolution-only loader.
import { createBootstrapHttpHandler, createBootstrapHttpTransport } from '../src/changeset-bootstrap-http.ts';
import { serveBootstrap } from './helpers/production-bootstrap-http-host.mjs';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

for (const ordered of [false, true]) {
  test(`actual HTTP ${ordered ? 'ordered' : 'unordered'} bootstrap resumes, loses an ACK, and preserves pending source payloads`, async () => {
    const { source, outbox } = await sourceWithSeed(12);
    const directory = mkdtempSync(join(tmpdir(), 'fsqlite-production-http-'));
    const path = join(directory, 'receiver.db');
    const destination = new SqliteTarget(path, schema);
    destination.db.exec('PRAGMA journal_mode=WAL');
    const observer = new SqliteTarget(path);
    let confirmations = 0, admissions = 0, loseAck = true, inspected = false;
    const receiver = new ChangesetBootstrapReceiver(destination, { receiverId: 'replica', tables: ['t'],
      ...(ordered ? { orderedSourceId: sourceId } : {}),
      confirmCommit: async () => { assert.equal(destination.active, false); confirmations++; },
    });
    destination.after = async (kind, sql) => {
      if (!inspected && kind === 'execute' && sql.startsWith('INSERT OR ABORT INTO main."t"')) {
        inspected = true; assert.deepEqual(observer.rows(), [], 'Independent reader must not see a seed prefix');
      }
    };
    const handler = createBootstrapHttpHandler(receiver, {
      authorize: request => { assert.equal(source.active, false, 'Network must run outside source SQL transactions'); admissions++; return request.headers.get('authorization') === 'Bearer test-secret'; },
      authorizeManifest: (_request, info) => info.manifest.deliveryId === root,
      ...(ordered ? { orderedSourceId: sourceId } : {}),
    });
    const host = await serveBootstrap(handler, async (request, response) => {
      if (loseAck && request.headers.get('x-fsqlite-bootstrap-action') === 'install' && response.status === 200) {
        loseAck = false; return true;
      }
      return false;
    });
    const transport = createBootstrapHttpTransport(host.url, { allowInsecureLoopback: true,
      headers: () => ({ authorization: 'Bearer test-secret' }),
      ...(ordered ? { orderedSourceId: sourceId } : {}),
    });
    const transfer = new ChangesetBootstrapTransfer(source, { receiverId: 'replica', deliveryId: root, tables: ['t'],
      transport, ...(ordered ? { orderedSourceId: sourceId } : {}), confirmSource: async () => { assert.equal(source.active, false); },
    });
    try {
      const before = pendingBytes(source);
      assert.equal((await transfer.run({ maxChunks: 3 })).stopped, 'limit');
      assert.equal(pendingBytes(source), before); assert.deepEqual(observer.rows(), []);
      await assert.rejects(transfer.run(), error => error.phase === 'install' && error.cause?.outcome === 'unknown');
      assert.ok(inspected); assert.deepEqual(observer.rows(), source.rows());
      assert.equal(pendingBytes(source), before); assert.equal((await outbox.pending()).length, 12);
      const result = await transfer.run();
      assert.equal(result.receipt.replayed, true); assert.equal(result.newlyAcknowledged, 12);
      assert.equal(result.uploadedChunks, 0); assert.equal(confirmations, 2);
      assert.equal(pendingBytes(source), 0n); assert.ok(admissions >= 17);
      assert.deepEqual(destination.rows(), source.rows());
    } finally { await host.close(); source.close(); observer.close(); destination.close(); }
  });
}

test('HTTP rejects damaged installed evidence instead of turning status(installed) into a source ACK', async () => {
  const { source } = await sourceWithSeed(4);
  const destination = new SqliteTarget(':memory:', schema);
  const { receiver } = connection(source, destination);
  const manifest = await readBootstrapManifest(source, { receiverId: 'replica', deliveryId: root, tables: ['t'] });
  const outbox = new ChangesetOutbox(source);
  for (let i = 0; i < manifest.chunks; i++) {
    const chunk = await outbox.read(i === 0 ? root : `${root}/chunk/${i}`);
    await receiver.stage(manifest, i, chunk.changeset);
  }
  await receiver.install(manifest);
  destination.db.exec('DELETE FROM __fsqlite_bootstrap_chunks WHERE idx=1');
  const host = await serveBootstrap(createBootstrapHttpHandler(receiver, { authorize: () => true }));
  const transport = createBootstrapHttpTransport(host.url, { allowInsecureLoopback: true });
  const transfer = new ChangesetBootstrapTransfer(source, { receiverId: 'replica', deliveryId: root, tables: ['t'], transport,
    confirmSource: async () => {},
  });
  try {
    assert.equal((await transport.status(manifest)).installed, true, 'Status remains only an observation');
    const before = pendingBytes(source);
    await assert.rejects(transfer.run(), error => error.phase === 'install' && error.cause?.status === 500);
    assert.equal(pendingBytes(source), before); assert.equal((await outbox.pending()).length, 4);
  } finally { await host.close(); source.close(); destination.close(); }
});

const childScript = fileURLToPath(new URL('./helpers/production-bootstrap-child.mjs', import.meta.url));
const loader = new URL('./helpers/production-source-loader.mjs', import.meta.url).href;
async function killedChild(config) {
  const child = spawn(process.execPath, ['--experimental-transform-types', `--experimental-loader=${loader}`, childScript, JSON.stringify(config)], {
    stdio: ['ignore', 'ignore', 'pipe'],
  });
  let stderr = '';
  child.stderr.on('data', chunk => { stderr += chunk; });
  const timer = setTimeout(() => child.kill('SIGTERM'), 10_000);
  try {
    const status = await new Promise((resolve, reject) => {
      child.once('error', reject); child.once('exit', (code, signal) => resolve({ code, signal }));
    });
    assert.equal(status.signal, 'SIGKILL', `${JSON.stringify(status)}\n${stderr}`);
  } finally { clearTimeout(timer); }
}

for (const mode of ['WAL', 'DELETE']) {
  for (const ordered of [false, true]) {
    for (const cut of ['stage-commit', 'application-row', 'install-before-commit', 'install-after-commit', 'ack-before-commit', 'ack-after-commit']) {
      test(`production ${mode} ${ordered ? 'ordered' : 'unordered'} SIGKILL at ${cut} recovers the same retained seed`, { timeout: 15_000 }, async () => {
        const directory = mkdtempSync(join(tmpdir(), 'fsqlite-production-kill-'));
        const sourcePath = join(directory, 'source.db'), destinationPath = join(directory, 'receiver.db');
        let source = new SqliteTarget(sourcePath, schema), destination = new SqliteTarget(destinationPath, schema);
        source.db.exec(`PRAGMA journal_mode=${mode}; PRAGMA synchronous=FULL;`);
        destination.db.exec(`PRAGMA journal_mode=${mode}; PRAGMA synchronous=FULL;`);
        for (let i = 1; i <= 3; i++) await source.execute('INSERT INTO t VALUES(?,?)', [BigInt(i), `row-${i}`]);
        await new ChangesetOutbox(source).bootstrapChunks({ deliveryId: root, tables: ['t'], chunkRows: 1 });
        source.close(); destination.close();
        await killedChild({ sourcePath, destinationPath, root, sourceId, ordered, cut });
        source = new SqliteTarget(sourcePath); destination = new SqliteTarget(destinationPath);
        try {
          const { receiver, transfer } = connection(source, destination, { ordered });
          const manifest = await readBootstrapManifest(source, { receiverId: 'replica', deliveryId: root, tables: ['t'] });
          const status = await receiver.status(manifest);
          const alreadyInstalled = ['install-after-commit', 'ack-before-commit', 'ack-after-commit'].includes(cut);
          assert.equal(status.installed, alreadyInstalled);
          assert.equal(status.receivedChunks, cut === 'stage-commit' ? 1 : 3);
          assert.equal(destination.rows().length, alreadyInstalled ? 3 : 0);
          const pending = await new ChangesetOutbox(source).pending();
          assert.equal(pending.length, cut === 'ack-after-commit' ? 0 : 3);
          destination.statements.length = 0;
          const result = await transfer.run();
          assert.equal(result.receipt.replayed, alreadyInstalled);
          assert.equal(result.newlyAcknowledged, cut === 'ack-after-commit' ? 0 : 3);
          assert.deepEqual(destination.rows(), source.rows());
          assert.equal(pendingBytes(source), 0n);
          if (alreadyInstalled) assert.ok(!destination.statements.some(sql => sql.startsWith('INSERT OR ABORT INTO main."t"')));
          assert.equal((await transfer.run()).newlyAcknowledged, 0);
        } finally { source.close(); destination.close(); }
      });
    }
  }
}

for (const encoding of ['UTF-8', 'UTF-16le']) {
  test(`production HTTP ${encoding} multi-table baseline preserves composite keys, int64, REAL, NUL and chunked BLOBs`, async () => {
    const ddl = `CREATE TABLE parents(id INTEGER PRIMARY KEY, body TEXT, payload BLOB, score REAL);
      CREATE TABLE children(group_key TEXT COLLATE NOCASE, ordinal INTEGER, parent_id INTEGER REFERENCES parents(id), body TEXT,
        PRIMARY KEY(group_key DESC, ordinal ASC)) WITHOUT ROWID;
      CREATE TABLE empty_table(id INTEGER PRIMARY KEY);`;
    const source = new SqliteTarget(); source.db.exec(`PRAGMA encoding='${encoding}';` + ddl);
    const destination = new SqliteTarget(':memory:', ddl), oracle = new SqliteTarget(':memory:', ddl);
    const tables = ['parents', 'children', 'empty_table'];
    const key = 9223372036854775600n;
    for (let i = 0; i < 128; i++) {
      await source.execute('INSERT INTO parents VALUES(?,?,?,CAST(? AS REAL))',
        [key + BigInt(i), `\uFEFFseed\0🙂-${i}`, new Uint8Array(8192).fill(i), i + 0.25]);
      await source.execute('INSERT INTO children VALUES(?,?,?,?)', [`Group-${i % 7}`, BigInt(i), key + BigInt(i), `child\0${i}`]);
    }
    const outbox = new ChangesetOutbox(source);
    const seed = await outbox.bootstrapChunks({ deliveryId: root, tables, chunkBytes: 64 * 1024, chunkRows: 25 });
    assert.ok(seed.chunks > 12); assert.ok(seed.byteLength > 1024 * 1024);
    // Independent native SQLite Session application oracle for generated source bytes.
    for (const entry of await outbox.pending({ limit: 256 })) {
      const read = await outbox.read(entry.deliveryId);
      assert.ok(read.changeset.byteLength <= 64 * 1024);
      assert.equal(oracle.db.applyChangeset(read.changeset), true);
    }
    let sourceRowReads = 0;
    source.before = async (kind, sql) => {
      if (kind === 'query' && /FROM main\."(parents|children|empty_table)"/.test(sql)) sourceRowReads++;
    };
    const receiver = new ChangesetBootstrapReceiver(destination, { receiverId: 'replica', tables,
      orderedSourceId: sourceId, maxChunkBytes: 64 * 1024, confirmCommit: async () => {},
    });
    const host = await serveBootstrap(createBootstrapHttpHandler(receiver, {
      authorize: () => true, orderedSourceId: sourceId, maxChunkBytes: 64 * 1024,
    }));
    const transport = createBootstrapHttpTransport(host.url, { allowInsecureLoopback: true,
      orderedSourceId: sourceId, maxChunkBytes: 64 * 1024,
    });
    const transfer = new ChangesetBootstrapTransfer(source, { receiverId: 'replica', deliveryId: root, tables,
      orderedSourceId: sourceId, transport, maxChunkBytes: 64 * 1024, confirmSource: async () => {},
    });
    const queries = [
      'SELECT id,CAST(body AS BLOB),payload,typeof(score),score FROM parents ORDER BY id',
      'SELECT group_key,ordinal,parent_id,CAST(body AS BLOB) FROM children ORDER BY group_key,ordinal',
      'SELECT * FROM empty_table',
    ];
    try {
      assert.equal((await transfer.run({ maxChunks: 3 })).stopped, 'limit');
      for (const sql of queries) assert.deepEqual(destination.rows(sql), []);
      const result = await transfer.run();
      assert.equal(result.stopped, 'installed'); assert.equal(result.newlyAcknowledged, seed.chunks);
      for (const sql of queries) assert.deepEqual(destination.rows(sql), oracle.rows(sql));
      assert.equal(sourceRowReads, 0, 'Transfer must not recapture newer source rows');
      assert.equal((await transfer.run()).uploadedChunks, 0);
      assert.equal((await new ChangesetOrder(destination, { receiverId: 'replica', sourceId }).head()).sequence, BigInt(seed.chunks));
    } finally { await host.close(); source.close(); destination.close(); oracle.close(); }
  });
}

for (const failureAt of ['receiver-confirm', 'source-confirm-after-ack']) {
  test(`actual ${failureAt} failure remains recoverable without replaying application SQL`, async () => {
    const { source, outbox } = await sourceWithSeed(3);
    const destination = new SqliteTarget(':memory:', schema);
    let once = true, receiverCalls = 0, sourceCalls = 0;
    const { transfer } = connection(source, destination, {
      confirmDestination: async () => { receiverCalls++; if (failureAt === 'receiver-confirm' && once) { once = false; throw new Error('confirmation unavailable'); } },
      confirmSource: async () => {
        sourceCalls++;
        if (failureAt === 'source-confirm-after-ack' && once && pendingBytes(source) === 0n) {
          once = false; throw new Error('source confirmation unavailable');
        }
      },
    });
    try {
      await assert.rejects(transfer.run(), error => error.phase === (failureAt === 'receiver-confirm' ? 'install' : 'source-confirm'));
      assert.deepEqual(destination.rows(), source.rows());
      assert.equal((await outbox.pending()).length, failureAt === 'receiver-confirm' ? 3 : 0);
      destination.statements.length = 0;
      const result = await transfer.run();
      assert.equal(result.receipt.replayed, true); assert.equal(receiverCalls, 2); assert.ok(sourceCalls > 2);
      assert.equal(result.newlyAcknowledged, failureAt === 'receiver-confirm' ? 3 : 0);
      assert.ok(!destination.statements.some(sql => sql.startsWith('INSERT OR ABORT INTO main."t"')));
      assert.equal(pendingBytes(source), 0n);
    } finally { source.close(); destination.close(); }
  });
}

for (const encoding of ['UTF-16le', 'UTF-16be']) {
  test(`production ${encoding} source and receiver support maximum identities through fanout transfer and ordered replay`, async () => {
    const source = new SqliteTarget(), destination = new SqliteTarget();
    source.db.exec(`PRAGMA encoding='${encoding}';` + schema);
    destination.db.exec(`PRAGMA encoding='${encoding}';` + schema);
    const id = 'r'.repeat(256), origin = 's'.repeat(256), deliveryId = 'b'.repeat(480);
    await ChangesetFanout.open(source, [id]);
    await source.execute("INSERT INTO t VALUES(1,'first')");
    const outbox = new ChangesetOutbox(source);
    await outbox.bootstrapChunks({ deliveryId, tables: ['t'], chunkRows: 1 });
    const receiver = new ChangesetBootstrapReceiver(destination, { receiverId: id, orderedSourceId: origin,
      tables: ['t'], confirmCommit: async () => {},
    });
    const transfer = new ChangesetBootstrapTransfer(source, { receiverId: id, deliveryId, tables: ['t'],
      orderedSourceId: origin, acknowledgement: 'fanout', transport: receiver, confirmSource: async () => {},
    });
    try {
      const receipt = await transfer.run();
      assert.equal(receipt.newlyAcknowledged, 1); assert.equal(receipt.receipt.order.sequence, '1');
      assert.deepEqual(destination.rows(), source.rows()); assert.equal(pendingBytes(source), 0n);
      assert.equal((await transfer.run()).newlyAcknowledged, 0);
      const ledger = new ChangesetOrder(destination, { receiverId: id, sourceId: origin });
      assert.equal((await ledger.head()).sequence, 1n);
    } finally { source.close(); destination.close(); }
  });
}
