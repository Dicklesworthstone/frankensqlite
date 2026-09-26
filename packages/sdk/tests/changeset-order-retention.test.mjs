import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { ChangesetOrder } from '../src/changeset-order.ts';
import { applyChangeset } from '../src/changeset-apply.ts';
import { ChangesetOutbox } from '../src/changeset-outbox.ts';
import { ChangesetBootstrapReceiver, readBootstrapManifest } from '../src/changeset-bootstrap.ts';
import { ChangesetBootstrapTransfer } from '../src/changeset-bootstrap-transfer.ts';
import { SqliteTarget, nativeSeed } from './helpers/production-sqlite-target.mjs';

const schema = 'CREATE TABLE t(id INTEGER PRIMARY KEY, value TEXT)';
const route = { receiverId: 'receiver', sourceId: 'source:incarnation' };
const ledger = '__fsqlite_changeset_order', retention = '__fsqlite_changeset_order_retention';
const apply = (inside, bytes) => applyChangeset(inside, bytes, { tables: ['t'] });
const code = expected => error => error?.code === `ERR_FSQLITE_ORDER_${expected}`;
const hash = async bytes => Buffer.from(await crypto.subtle.digest('SHA-256', bytes)).toString('hex');
async function messages(n = 8, identity = 'source:delivery:') {
  return Promise.all(nativeSeed(n).map(async (changeset, i) => Object.freeze({
    sequence: BigInt(i + 1), deliveryId: `${identity}${i + 1}`, sha256: await hash(changeset), changeset,
  })));
}
function keys(target) { return target.rows(`SELECT seq FROM ${ledger} ORDER BY seq`).flat(); }
function hasRetention(target) {
  return target.rows('SELECT 1 FROM sqlite_schema WHERE name=?', [retention]).length !== 0;
}
async function setup(n = 5, options = {}, path = ':memory:', encoding = 'UTF-8') {
  const target = new SqliteTarget(path);
  target.db.exec(`PRAGMA encoding='${encoding}'; ${schema}`);
  const order = new ChangesetOrder(target, { ...route, ...options });
  const list = await messages(Math.max(8, n));
  await order.initialize();
  for (const message of list.slice(0, n)) await order.apply(message, apply);
  return { target, order, list };
}

for (const encoding of ['UTF-8', 'UTF-16le', 'UTF-16be']) {
  test(`${encoding}: retirement frees a full ledger without resetting sequences or rerunning SQL`, async () => {
    const { target, order, list } = await setup(5, { maxEntries: 5 }, ':memory:', encoding);
    try {
      await assert.rejects(order.apply(list[5], apply), code('FULL'));
      const before = target.rows(), head = await order.head();
      assert.deepEqual(await order.retireBefore(4n), {
        removed: 3, retiredBefore: 4n, protectedThrough: 0n, retainedEntries: 2,
      });
      assert.deepEqual(target.rows(), before); assert.deepEqual(await order.head(), head);
      assert.deepEqual(keys(target), [0n, 4n, 5n]);
      assert.equal((await order.apply(list[3], () => { throw new Error('Replay invoked callback'); })).replayed, true);
      let called = 0;
      await assert.rejects(order.apply(list[0], async () => { called++; return {}; }), code('EXPIRED'));
      assert.equal(called, 0);
      await order.apply(list[5], apply);
      assert.equal((await order.head()).sequence, 6n); assert.equal(target.rows().length, 6);
      assert.equal((await order.retireBefore(4n)).removed, 0);
      assert.equal((await order.retireBefore(2n)).retiredBefore, 4n);
      assert.equal((await order.retireBefore(6n)).removed, 2);
      assert.deepEqual(keys(target), [0n, 6n]);
    } finally { target.close(); }
  });
}

test('many more deliveries than the configured capacity remain bounded with explicit retirement', async () => {
  const target = new SqliteTarget(':memory:', schema), list = await messages(120);
  const order = new ChangesetOrder(target, { ...route, maxEntries: 4 });
  try {
    await order.initialize();
    for (const message of list) {
      if (keys(target).length === 5) await order.retireBefore((await order.head()).sequence);
      assert.equal((await order.apply(message, apply)).replayed, false);
      assert.ok(keys(target).length <= 5);
    }
    assert.equal((await order.head()).sequence, 120n);
    assert.equal(target.rows().length, 120);
    await assert.rejects(order.apply(list[0], apply), code('EXPIRED'));
    assert.equal((await order.apply(list[119], apply)).replayed, true);
  } finally { target.close(); }
});

test('no-op retirement keeps legacy schema and protected identities unchanged', async () => {
  const { target, order } = await setup(2);
  try {
    assert.equal((await order.retireBefore(1n)).removed, 0);
    assert.equal(hasRetention(target), false); assert.deepEqual(keys(target), [0n, 1n, 2n]);
    await assert.rejects(order.retireBefore(3n), code('GAP'));
    for (const value of [0n, -1n, 1n << 63n, 2, '2', null, undefined])
      await assert.rejects(order.retireBefore(value), code('INPUT'));
    assert.equal(hasRetention(target), false);
  } finally { target.close(); }
});

for (const encoding of ['UTF-8', 'UTF-16le', 'UTF-16be']) {
  test(`${encoding}: installed seed proof survives retirement and later increments`, async () => {
    const source = new SqliteTarget(), destination = new SqliteTarget();
    source.db.exec(`PRAGMA encoding='${encoding}'; ${schema}`);
    destination.db.exec(`PRAGMA encoding='${encoding}'; ${schema}`);
    const outbox = new ChangesetOutbox(source), root = 'source:seed';
    try {
      source.db.exec("INSERT INTO t VALUES(1,'seed-1'),(2,'seed-2')");
      await outbox.bootstrapChunks({ deliveryId: root, tables: ['t'], chunkRows: 1 });
      const receiver = new ChangesetBootstrapReceiver(destination, { receiverId: route.receiverId, tables: ['t'],
        orderedSourceId: route.sourceId, confirmCommit: async () => {},
      });
      const transfer = new ChangesetBootstrapTransfer(source, { receiverId: route.receiverId, deliveryId: root,
        tables: ['t'], orderedSourceId: route.sourceId, transport: receiver, confirmSource: async () => {},
      });
      const installed = await transfer.run(), order = new ChangesetOrder(destination, { ...route, maxEntries: 5 });
      for (let i = 3; i <= 8; i++) {
        const recorded = await outbox.record(tx => tx.execute('INSERT INTO t VALUES(?,?)', [BigInt(i), `next-${i}`]),
          { deliveryId: `source:next:${i}`, tables: ['t'] });
        const loaded = await outbox.read(recorded.delivery.deliveryId);
        if (keys(destination).length === 6) await order.retireBefore((await order.head()).sequence);
        await order.apply({ ...loaded.delivery, changeset: loaded.changeset }, apply);
        await outbox.acknowledge(loaded.delivery.deliveryId, loaded.delivery.sha256);
      }
      assert.deepEqual(keys(destination).slice(0, 3), [0n, 1n, 2n]);
      const before = destination.rows(), head = await order.head();
      assert.equal((await order.retireBefore(2n)).protectedThrough, 2n);
      const replay = await receiver.install(installed.manifest);
      assert.equal(replay.replayed, true); assert.equal(replay.order.sequence, '2');
      assert.deepEqual(destination.rows(), before); assert.deepEqual(await order.head(), head);
      assert.equal((await transfer.run()).newlyAcknowledged, 0);
      assert.deepEqual(destination.rows(), source.rows());
    } finally { source.close(); destination.close(); }
  });
}

test('retirement refuses a staged baseline before any application is installed', async () => {
  const { target, order, list } = await setup(0);
  const source = new SqliteTarget(':memory:', schema), outbox = new ChangesetOutbox(source);
  try {
    source.db.exec("INSERT INTO t VALUES(1,'seed-0')");
    await outbox.bootstrapChunks({ deliveryId: 'root', tables: ['t'], chunkRows: 1 });
    const manifest = await readBootstrapManifest(source, { receiverId: route.receiverId, deliveryId: 'root', tables: ['t'] });
    const chunk = await outbox.read('root');
    const receiver = new ChangesetBootstrapReceiver(target, { ...route, tables: ['t'], orderedSourceId: route.sourceId, confirmCommit: async () => {} });
    await receiver.stage(manifest, 0, chunk.changeset);
    await assert.rejects(order.retireBefore(1n), code('GAP'));
    await assert.rejects(order.apply(list[0], apply), code('GAP'));
    assert.equal(hasRetention(target), false);
  } finally { source.close(); target.close(); }
});

for (const mutation of [
  `DELETE FROM ${retention}`,
  `DROP TABLE ${retention}`,
  `DELETE FROM ${ledger} WHERE seq=0`,
  `DELETE FROM ${ledger} WHERE seq=4`,
  `DELETE FROM ${ledger} WHERE seq=5`,
  `UPDATE ${ledger} SET applied=2 WHERE seq=4`,
  `UPDATE ${retention} SET receipt='{}'`,
  `UPDATE ${retention} SET receipt=CAST(receipt AS BLOB)`,
  `UPDATE ${retention} SET receipt=receipt||char(0)||'tail'`,
  `UPDATE ${retention} SET receipt=json_set(receipt,'$.protectedThrough','4')`,
  `UPDATE ${retention} SET receipt=json_set(receipt,'$.sourceId','other')`,
  `UPDATE ${retention} SET receipt=json_set(receipt,'$.version',2)`,
  `UPDATE ${ledger} SET seq=2 WHERE seq=5`,
  `UPDATE ${ledger} SET seq=3 WHERE seq=4`,
]) {
  test(`damaged retirement fails closed: ${mutation}`, async () => {
    const { target, order, list } = await setup(6);
    try {
      await order.retireBefore(4n); target.db.exec(mutation);
      const rows = target.rows(); let invoked = false;
      await assert.rejects(order.head(), error => /ERR_FSQLITE_ORDER_(CORRUPT|BINDING)/.test(error.code));
      await assert.rejects(order.initialize());
      await assert.rejects(order.retireBefore(6n));
      await assert.rejects(order.apply(list[6], async () => { invoked = true; return {}; }));
      assert.equal(invoked, false); assert.deepEqual(target.rows(), rows);
    } finally { target.close(); }
  });
}

for (const ddl of [
  `CREATE VIEW ${retention} AS SELECT 1 AS slot, '{}' AS receipt`,
  `CREATE TABLE ${retention}(slot INTEGER PRIMARY KEY,receipt BLOB NOT NULL)`,
  `CREATE TABLE ${retention}(slot INTEGER PRIMARY KEY,receipt TEXT NOT NULL); CREATE INDEX rx ON ${retention}(receipt)`,
  `CREATE TABLE ${retention}(slot INTEGER PRIMARY KEY,receipt TEXT NOT NULL); CREATE TRIGGER rx AFTER INSERT ON ${retention} BEGIN SELECT 1; END`,
]) {
  test(`invalid retirement schema never authorizes receipt deletion: ${ddl}`, async () => {
    const { target, order } = await setup();
    try {
      target.db.exec(ddl); const before = keys(target);
      await assert.rejects(order.retireBefore(4n), code('SCHEMA'));
      assert.deepEqual(keys(target), before);
    } finally { target.close(); }
  });
}

test('leftover retention evidence cannot be mistaken for a new stream', async () => {
  const { target, order } = await setup();
  try {
    await order.retireBefore(4n);
    target.db.exec(`DROP TABLE ${ledger}; DROP TABLE __fsqlite_changeset_order_head`);
    await assert.rejects(order.initialize(), code('CORRUPT'));
    assert.equal(target.rows("SELECT 1 FROM sqlite_schema WHERE name=?", [ledger]).length, 0);
  } finally { target.close(); }
});

test('exact retained identities still reject content and identity reuse', async () => {
  const { target, order, list } = await setup();
  try {
    await order.retireBefore(4n);
    await assert.rejects(order.apply({ ...list[3], deliveryId: 'forged' }, apply), code('REUSE'));
    await assert.rejects(order.apply({ ...list[5], deliveryId: list[4].deliveryId }, apply), code('REUSE'));
    assert.deepEqual(keys(target), [0n, 4n, 5n]);
  } finally { target.close(); }
});

for (const cut of ['anchor-insert', 'receipt-delete']) {
  test(`cancellation after ${cut} rolls back the whole retirement`, async () => {
    const { target, order } = await setup(), controller = new AbortController();
    try {
      target.after = async (kind, sql) => {
        if (kind === 'execute' && (cut === 'anchor-insert' ? sql.startsWith(`INSERT OR ABORT INTO main."${retention}"`) : sql.startsWith(`DELETE FROM main."${ledger}"`))) controller.abort();
      };
      await assert.rejects(order.retireBefore(4n, { signal: controller.signal }), code('CANCELLED'));
      assert.equal(hasRetention(target), false); assert.deepEqual(keys(target), [0n,1n,2n,3n,4n,5n]);
      target.after = null; assert.equal((await order.retireBefore(4n)).removed, 3);
    } finally { target.close(); }
  });
}

test('incorrect affected-row report, outer rollback and deferred COMMIT failure restore both ranges', async () => {
  const { target, order } = await setup();
  try {
    const sourceExecute = target.execute.bind(target);
    target.execute = async (sql, params) => { const n = await sourceExecute(sql, params); return sql.startsWith(`DELETE FROM main."${ledger}"`) ? n - 1 : n; };
    await assert.rejects(order.retireBefore(4n), code('CORRUPT'));
    target.execute = sourceExecute;
    assert.equal(hasRetention(target), false);
    await assert.rejects(target.transaction(async tx => {
      const nested = new ChangesetOrder({ transaction: work => work(tx) }, route);
      await nested.retireBefore(4n); throw new Error('outer rollback');
    }), /outer rollback/);
    assert.equal(hasRetention(target), false);
    target.db.exec('CREATE TABLE p(id PRIMARY KEY); CREATE TABLE c(id REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)');
    target.beforeCommit = async () => target.db.exec('INSERT INTO c VALUES(99)');
    await assert.rejects(order.retireBefore(4n), /FOREIGN KEY/);
    target.beforeCommit = null;
    assert.equal(hasRetention(target), false); assert.deepEqual(keys(target), [0n,1n,2n,3n,4n,5n]);
  } finally { target.close(); }
});

test('application callbacks cannot change retirement while advancing the ledger', async () => {
  const { target, order, list } = await setup();
  try {
    await order.retireBefore(3n); const before = keys(target);
    await assert.rejects(order.apply(list[5], async inside => {
      await inside.transaction(async tx => {
        const other = new ChangesetOrder({ transaction: work => work(tx) }, route);
        await other.retireBefore(5n);
      });
      return apply(inside, list[5].changeset);
    }), code('CORRUPT'));
    assert.deepEqual(keys(target), before); assert.equal(target.rows().length, 5);
  } finally { target.close(); }
});

test('concurrent retirement owners preserve monotonic bounds without replay or writer serialization', async () => {
  const directory = mkdtempSync(join(tmpdir(), 'fsqlite-retire-race-')), path = join(directory, 'data.db');
  const { target: a, order: first } = await setup(6, {}, path); a.db.exec('PRAGMA journal_mode=WAL');
  const b = new SqliteTarget(path), second = new ChangesetOrder(b, route);
  let release, reached; const held = new Promise(resolve => { release = resolve; }), ready = new Promise(resolve => { reached = resolve; });
  let once = true;
  a.before = async (kind, sql) => { if (once && kind === 'query' && sql.startsWith('SELECT count(*), CAST(min(seq)')) { once = false; reached(); await held; } };
  try {
    const losing = first.retireBefore(5n); const refused = assert.rejects(losing);
    await ready; assert.equal((await second.retireBefore(4n)).removed, 3); release(); await refused;
    a.before = null;
    assert.deepEqual(keys(a), [0n,4n,5n,6n]); assert.equal((await first.retireBefore(5n)).removed, 1);
    assert.deepEqual(keys(a), [0n,5n,6n]); assert.equal(a.rows().length, 6);
  } finally { release(); a.close(); b.close(); }
});

for (const encoding of ['UTF-16le', 'UTF-16be']) {
  test(`${encoding}: maximum escaped identities fit bounded retirement anchors`, async () => {
    const target = new SqliteTarget(); target.db.exec(`PRAGMA encoding='${encoding}'; ${schema}`);
    const order = new ChangesetOrder(target, { receiverId: '\x01'.repeat(256), sourceId: '\x02'.repeat(256), maxEntries: 3 });
    const list = await messages(4, '\x03'.repeat(510));
    try {
      await order.initialize(); for (const m of list.slice(0,3)) await order.apply(m, apply);
      await order.retireBefore(3n); await order.apply(list[3], apply);
      assert.equal((await order.head()).sequence, 4n);
      assert.ok(target.rows(`SELECT length(CAST(receipt AS BLOB)) FROM ${retention}`)[0][0] <= 16384n);
    } finally { target.close(); }
  });
}

const childScript = fileURLToPath(new URL('./helpers/order-retention-child.mjs', import.meta.url));
const loader = new URL('./helpers/production-source-loader.mjs', import.meta.url).href;
for (const journal of ['WAL', 'DELETE']) for (const cut of ['anchor', 'delete', 'before-commit', 'after-commit']) {
  test(`${journal}: actual SIGKILL ${cut} preserves an atomic retirement decision on reopen`, { timeout: 15_000 }, async () => {
    const directory = mkdtempSync(join(tmpdir(), 'fsqlite-retire-kill-')), path = join(directory, 'data.db');
    let { target, order, list } = await setup(5, {}, path);
    target.db.exec(`PRAGMA journal_mode=${journal}; PRAGMA synchronous=FULL`); target.close();
    const child = spawn(process.execPath, ['--experimental-transform-types', `--experimental-loader=${loader}`, childScript, path, cut], { stdio: ['ignore','ignore','pipe'] });
    let stderr = ''; child.stderr.on('data', data => { stderr += data; });
    const timer = setTimeout(() => child.kill('SIGTERM'), 10_000);
    try {
      const status = await new Promise((resolve, reject) => { child.once('error', reject); child.once('exit', (code, signal) => resolve({ code, signal })); });
      assert.equal(status.signal, 'SIGKILL', stderr);
    } finally { clearTimeout(timer); }
    target = new SqliteTarget(path); order = new ChangesetOrder(target, route);
    try {
      assert.equal((await order.head()).sequence, 5n); assert.equal(target.rows().length, 5);
      assert.deepEqual(keys(target), cut === 'after-commit' ? [0n,4n,5n] : [0n,1n,2n,3n,4n,5n]);
      assert.equal((await order.retireBefore(4n)).removed, cut === 'after-commit' ? 0 : 3);
      await order.apply(list[5], apply); assert.equal(target.rows().length, 6);
      await assert.rejects(order.apply(list[0], apply), code('EXPIRED'));
    } finally { target.close(); }
  });
}

test('wrong binding and pre-aborted admission never mutate retirement state', async () => {
  const { target, order } = await setup();
  try {
    const wrong = new ChangesetOrder(target, { ...route, sourceId: 'another source' });
    await assert.rejects(wrong.retireBefore(4n), code('BINDING'));
    const controller = new AbortController(); controller.abort('stop'); target.statements.length = 0;
    await assert.rejects(order.retireBefore(4n, { signal: controller.signal }), code('CANCELLED'));
    assert.deepEqual(target.statements, []); assert.equal(hasRetention(target), false);
  } finally { target.close(); }
});

test('lost retirement commit acknowledgement recovers without restoring expired history', async () => {
  const { target, order, list } = await setup();
  try {
    target.afterCommit = async () => { throw new Error('lost commit response'); };
    await assert.rejects(order.retireBefore(4n), /lost commit response/);
    target.afterCommit = null;
    assert.deepEqual(keys(target), [0n,4n,5n]); assert.equal((await order.retireBefore(4n)).removed, 0);
    await assert.rejects(order.apply(list[1], apply), code('EXPIRED'));
    await order.apply(list[5], apply); assert.equal(target.rows().length, 6);
  } finally { target.close(); }
});

test('one instance rejects overlap while retaining and draining the admitted retirement', async () => {
  const { target, order, list } = await setup();
  let release, reached; const held = new Promise(resolve => { release = resolve; }), ready = new Promise(resolve => { reached = resolve; });
  let once = true;
  target.before = async (kind, sql) => { if (once && kind === 'query') { once = false; reached(); await held; } };
  try {
    const active = order.retireBefore(4n); await ready;
    await assert.rejects(order.retireBefore(5n), code('BUSY'));
    await assert.rejects(order.apply(list[5], apply), code('BUSY'));
    release(); assert.equal((await active).removed, 3);
    target.before = null; await order.apply(list[5], apply);
  } finally { release(); target.close(); }
});

test('deadline during a read aborts retirement before publication', async () => {
  const { target, order } = await setup(); let once = true;
  target.before = async kind => { if (once && kind === 'query') { once = false; await new Promise(resolve => setTimeout(resolve, 25)); } };
  try {
    await assert.rejects(order.retireBefore(4n, { timeoutMs: 5 }), code('TIMEOUT'));
    assert.equal(hasRetention(target), false); assert.deepEqual(keys(target), [0n,1n,2n,3n,4n,5n]);
  } finally { target.close(); }
});

for (const mutation of [
  `UPDATE ${ledger} SET source_id='foreign' WHERE seq=2`,
  `UPDATE ${ledger} SET sha256='invalid' WHERE seq=2`,
  `UPDATE ${ledger} SET byte_length=-1 WHERE seq=2`,
]) {
  test(`retirement validates old interior evidence before deleting it: ${mutation}`, async () => {
    const { target, order } = await setup();
    try {
      target.db.exec(mutation); const before = keys(target);
      await assert.rejects(order.retireBefore(5n), code('CORRUPT'));
      assert.deepEqual(keys(target), before); assert.equal(hasRetention(target), false);
    } finally { target.close(); }
  });
}

test('retiring 127 identities validates bounded keyset pages using the sequence index', async () => {
  const { target, order } = await setup(128); let pages = 0;
  target.after = async (kind, sql, params, result) => {
    if (kind === 'query' && sql.includes('WHERE seq>=? AND seq<? ORDER BY seq LIMIT 32')) {
      pages++; assert.ok(result.rowArrays.length <= 32);
      const plan = target.db.prepare(`EXPLAIN QUERY PLAN ${sql}`).all(...params);
      assert.ok(plan.some(row => /SEARCH.*INTEGER PRIMARY KEY/.test(row.detail)));
      assert.ok(!plan.some(row => /TEMP B-TREE/.test(row.detail)));
    }
  };
  try {
    assert.equal((await order.retireBefore(128n)).removed, 127);
    assert.equal(pages, 4); assert.deepEqual(keys(target), [0n,128n]);
  } finally { target.close(); }
});
