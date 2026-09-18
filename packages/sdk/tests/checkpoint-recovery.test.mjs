// Real Node worker messages through production SDK/host/store. Node SQLite and
// the explicit IDB model are test adapters; this does NOT certify browser/WASM.
import assert from 'node:assert/strict';
import test from 'node:test';
import { Worker } from 'node:worker_threads';
import { setImmediate as turn } from 'node:timers/promises';
import { mkdtemp, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { DatabaseSync } from 'node:sqlite';
import { FrankenDB } from '../src/database.ts';
import { FrankenDBQueue, FrankenCheckpointCommitError } from '../src/queue.ts';

function transport(t, workerData = {}) {
  const thread = new Worker(new URL('../../worker/tests/helpers/checkpoint-thread.mjs', import.meta.url), { workerData });
  const listeners = { message: new Set(), error: new Set(), messageerror: new Set() };
  const controls = new Map(), notices = new Map(); let id = 0, termination;
  const worker = { sent: [],
    addEventListener(type, listener) { listeners[type].add(listener); },
    removeEventListener(type, listener) { listeners[type].delete(listener); },
    postMessage(request, transfer) { worker.sent.push(structuredClone(request)); thread.postMessage(request, transfer); },
    terminate() { termination ??= thread.terminate(); },
    control(testControl, value) { return new Promise((resolve, reject) => {
      const n = ++id; controls.set(n, { resolve, reject }); thread.postMessage({ testControl, id: n, value });
    }); },
    notice(name) { return new Promise(resolve => { notices.set(name, resolve); }); },
  };
  thread.on('message', data => {
    if (data.testControl) {
      const c = controls.get(data.id); controls.delete(data.id);
      if (data.error) c?.reject(new Error(data.error)); else c?.resolve(data.result);
      return;
    }
    if (data.notice) { notices.get(data.notice)?.(); notices.delete(data.notice); return; }
    for (const l of listeners.message) l({ data });
  });
  thread.on('error', error => {
    for (const c of controls.values()) c.reject(error); controls.clear();
    for (const l of listeners.error) l({ message: error.message });
  });
  thread.on('messageerror', () => { for (const l of listeners.messageerror) l(); });
  t.after(async () => { termination ??= thread.terminate(); await termination; });
  return worker;
}
async function open(t, queue = false, workerData = {}, queueOptions = {}) {
  const worker = transport(t, workerData), options = { worker, dbName: crypto.randomUUID(), persistence: 'indexeddb-snapshot' };
  const db = queue ? await FrankenDBQueue.open(options, { checkpointOnCommit: true, ...queueOptions }) : await FrankenDB.open(options);
  await db.transaction(tx => tx.execute('CREATE TABLE items(value INTEGER)'));
  return { db, worker };
}
async function inspectImage(bytes) {
  const path = join(await mkdtemp(join(tmpdir(), 'fsqlite-receipt-')), 'saved.sqlite');
  await writeFile(path, new Uint8Array(bytes)); const raw = new DatabaseSync(path);
  try {
    assert.equal(raw.prepare('PRAGMA integrity_check').get().integrity_check, 'ok');
    return raw.prepare('SELECT * FROM items').all().map(row => row.value);
  } finally { raw.close(); }
}
const receiptFailure = e => e.code === 'ERR_FSQLITE_SNAPSHOT_RECEIPT';

for (const fault of ['store-ack', 'worker-ack', 'bad-receipt', 'bad-envelope', 'wrong-token', 'wrong-parent']) {
  test(`real thread recovers ${fault} without export, publication or repeated SQL`, async t => {
    const { db, worker } = await open(t);
    assert.equal(db.checkpointRecoverySupported, true);
    await db.execute('INSERT INTO items VALUES(7)');
    await worker.control('fault', fault);
    await assert.rejects(db.checkpoint());
    assert.equal(db.snapshotRevision, null);
    const before = await worker.control('stats'), id = worker.sent.findLast(r => r.kind === 'checkpoint').publicationId;
    const saved = await db.recoverCheckpoint();
    assert.equal(saved.revision, id); assert.equal(saved.parentRevision, null); assert.ok(Object.isFrozen(saved));
    assert.equal(db.snapshotRevision, id);
    const after = await worker.control('stats'); assert.equal(after.saves, before.saves); assert.deepEqual(after.events, before.events);
    assert.deepEqual(after.head, before.head); assert.deepEqual(await inspectImage(after.head.bytes), [7]);
    await db.execute('INSERT INTO items VALUES(8)'); const next = await db.checkpoint(); assert.equal(next.parentRevision, id);
    await db.close();
  });
}

test('successful normal checkpoints retain concurrent FIFO lineage', async t => {
  const { db, worker } = await open(t);
  const saved = await Promise.all([db.checkpoint(), db.checkpoint(), db.checkpoint()]);
  assert.equal(saved[0].parentRevision, null);
  assert.equal(saved[1].parentRevision, saved[0].revision); assert.equal(saved[2].parentRevision, saved[1].revision);
  const requests = worker.sent.filter(r => r.kind === 'checkpoint');
  assert.deepEqual(saved.map(r => r.revision), requests.map(r => r.publicationId));
  await assert.rejects(db.recoverCheckpoint(), e => e.code === 'ERR_FSQLITE_SNAPSHOT_RECOVERY_EMPTY');
  await db.close();
});

for (const corruption of ['bytes', 'revision', 'absent']) {
  test(`recovery refuses authoritative ${corruption} damage and leaves the receipt fence`, async t => {
    const { db, worker } = await open(t);
    await worker.control('fault', 'bad-receipt'); await assert.rejects(db.checkpoint(), receiptFailure);
    await worker.control('corrupt', corruption); const before = await worker.control('stats');
    await assert.rejects(db.recoverCheckpoint(), e => e.code ===
      (corruption === 'bytes' ? 'ERR_FSQLITE_SNAPSHOT_CORRUPT' : 'ERR_FSQLITE_SNAPSHOT_NOT_CONFIRMED'));
    await assert.rejects(db.checkpoint(), receiptFailure); assert.equal(db.snapshotRevision, null);
    const after = await worker.control('stats'); assert.deepEqual(after.head, before.head); assert.equal(after.saves, before.saves);
    await db.close();
  });
}

for (const fault of ['bad-receipt', 'bad-envelope', 'wrong-token', 'wrong-parent', 'worker-ack']) {
  test(`a second ${fault} during recovery retains the same candidate for another readback`, async t => {
    const { db, worker } = await open(t);
    await worker.control('fault', 'bad-receipt'); await assert.rejects(db.checkpoint(), receiptFailure);
    const publication = worker.sent.findLast(r => r.kind === 'checkpoint').publicationId;
    await worker.control('fault', fault); await assert.rejects(db.recoverCheckpoint()); assert.equal(db.snapshotRevision, null);
    await assert.rejects(db.checkpoint(), receiptFailure);
    const receipt = await db.recoverCheckpoint(); assert.equal(receipt.revision, publication);
    assert.equal((await worker.control('stats')).saves, 1); await db.close();
  });
}

test('overlapping uncertain checkpoint replies cannot skip an unacknowledged revision', async t => {
  const { db, worker } = await open(t); await worker.control('fault', 'bad-receipt');
  const results = await Promise.allSettled([db.checkpoint(), db.checkpoint()]);
  assert.ok(results.every(r => r.status === 'rejected'));
  await assert.rejects(db.recoverCheckpoint(), e => e.code === 'ERR_FSQLITE_SNAPSHOT_NOT_CONFIRMED');
  assert.equal(db.snapshotRevision, null); await assert.rejects(db.checkpoint(), receiptFailure); await db.close();
});

test('no in-flight checkpoint is abandoned to admit recovery', async t => {
  const { db } = await open(t);
  const pending = db.checkpoint();
  await assert.rejects(db.recoverCheckpoint(), e => e.code === 'ERR_FSQLITE_SNAPSHOT_RECOVERY_PENDING');
  await pending; await db.close();
});

test('simultaneous recovery callers share one read; new publications wait for it', async t => {
  const { db, worker } = await open(t); await worker.control('fault', 'bad-receipt');
  await assert.rejects(db.checkpoint(), receiptFailure);
  await worker.control('hold'); const held = worker.notice('confirm-held');
  const a = db.recoverCheckpoint(), b = db.recoverCheckpoint(); assert.equal(a, b); await held;
  await assert.rejects(db.checkpoint(), receiptFailure);
  assert.equal(worker.sent.filter(r => r.kind === 'checkpoint-recover').length, 1);
  await worker.control('release'); await a; await db.close();
});

test('readback acknowledges the captured image, never later unsaved memory writes', async t => {
  const { db, worker } = await open(t);
  await db.execute('INSERT INTO items VALUES(1)'); await worker.control('fault', 'worker-ack'); await assert.rejects(db.checkpoint());
  await db.execute('INSERT INTO items VALUES(2)');
  await db.recoverCheckpoint(); const state = await worker.control('stats');
  assert.deepEqual(await inspectImage(state.head.bytes), [1]);
  assert.deepEqual((await db.query('SELECT value FROM items')).rowArrays, [[1], [2]]);
  await db.checkpoint(); assert.deepEqual(await inspectImage((await worker.control('stats')).head.bytes), [1, 2]); await db.close();
});

test('older workers preserve ordinary checkpoints and never falsely advertise recovery', async t => {
  const { db, worker } = await open(t, false, { legacy: true });
  assert.equal(db.checkpointRecoverySupported, false);
  await db.checkpoint(); assert.equal(worker.sent.findLast(r => r.kind === 'checkpoint').publicationId, undefined);
  await assert.rejects(db.recoverCheckpoint(), e => e.code === 'ERR_FSQLITE_SNAPSHOT_RECOVERY_UNAVAILABLE'); await db.close();
});

for (const capability of [false, 0, 2, '1', {}]) {
  test(`unsupported recovery capability ${JSON.stringify(capability)} fails opening`, async t => {
    const worker = transport(t, { capability });
    await assert.rejects(FrankenDB.open({ worker, dbName: crypto.randomUUID(), persistence: 'indexeddb-snapshot' }), receiptFailure);
  });
}

test('managed ownership applies to database recovery before any readback request', async t => {
  const { db, worker } = await open(t); await worker.control('fault', 'bad-receipt'); await assert.rejects(db.checkpoint());
  await db.transaction(async () => { await assert.rejects(db.recoverCheckpoint(), e => e.code === 'ERR_FSQLITE_TRANSACTION_OWNERSHIP'); });
  assert.equal(worker.sent.filter(r => r.kind === 'checkpoint-recover').length, 0);
  await db.recoverCheckpoint(); await db.close();
});

for (const fault of ['store-ack', 'bad-receipt']) {
  test(`queued ${fault} recovery clears the commit fence without replay or duplicate notifications`, async t => {
    const { db: queue, worker } = await open(t, true); const changes = [];
    const subscription = await queue.subscribe(['items'], change => changes.push(change));
    await worker.control('fault', fault); let calls = 0, committed;
    try { await queue.transaction(async tx => { calls++; await tx.execute('INSERT INTO items VALUES(9)'); return 'kept'; }); }
    catch (error) { committed = error; }
    assert.ok(committed instanceof FrankenCheckpointCommitError); assert.equal(committed.value, 'kept');
    assert.equal(queue.stats.checkpointRecoveryRequired, true);
    await assert.rejects(queue.transaction(() => { calls++; }), e => /RECOVERY/.test(e.code));
    const before = await worker.control('stats'), sequence = queue.changeSequence;
    const saved = await queue.recoverCheckpoint(); assert.equal(queue.snapshotRevision, saved.revision);
    assert.equal(queue.stats.checkpointRecoveryRequired, false); assert.equal(queue.changeSequence, sequence);
    const after = await worker.control('stats'); assert.equal(after.saves, before.saves); assert.deepEqual(after.events, before.events);
    assert.equal(calls, 1); await turn(); assert.equal(changes.length, 1);
    await queue.transaction(tx => tx.execute('INSERT INTO items VALUES(10)'));
    assert.deepEqual(await inspectImage((await worker.control('stats')).head.bytes), [9, 10]);
    subscription.unsubscribe(); await subscription.done; await queue.close();
  });
}

test('quota failure is not confirmed; explicit new publication still recovers the queue', async t => {
  const { db: queue, worker } = await open(t, true); await worker.control('fault', 'quota');
  await assert.rejects(queue.transaction(tx => tx.execute('INSERT INTO items VALUES(6)')), FrankenCheckpointCommitError);
  await assert.rejects(queue.recoverCheckpoint(), e => e.code === 'ERR_FSQLITE_SNAPSHOT_NOT_CONFIRMED');
  assert.equal(queue.stats.checkpointRecoveryRequired, true); await queue.checkpoint();
  assert.equal(queue.stats.checkpointRecoveryRequired, false);
  assert.deepEqual(await inspectImage((await worker.control('stats')).head.bytes), [6]); await queue.close();
});

test('queue holds capacity and drains active recovery before close, including late abort', async t => {
  const { db: queue, worker } = await open(t, true, {}, { maxPendingJobs: 1 });
  await worker.control('fault', 'bad-receipt');
  await assert.rejects(queue.transaction(tx => tx.execute('INSERT INTO items VALUES(5)')), FrankenCheckpointCommitError);
  await worker.control('hold'); const held = worker.notice('confirm-held'), controller = new AbortController();
  const recovery = queue.recoverCheckpoint({ signal: controller.signal }); await held;
  assert.equal(queue.stats.pendingJobs, 1); controller.abort('too late to abandon read');
  await assert.rejects(queue.export(), e => e.code === 'ERR_FSQLITE_JOB_QUEUE_FULL');
  let settled = false; const close = queue.close().then(() => { settled = true; });
  await turn(); assert.equal(settled, false); await worker.control('release'); await recovery; await close;
  assert.equal(queue.stats.state, 'closed'); assert.equal(queue.stats.pendingJobs, 0);
});

test('pre-aborted queued recovery runs no storage work and cannot clear the fence', async t => {
  const { db: queue, worker } = await open(t, true); await worker.control('fault', 'bad-receipt');
  await assert.rejects(queue.transaction(tx => tx.execute('INSERT INTO items VALUES(5)')), FrankenCheckpointCommitError);
  await assert.rejects(queue.recoverCheckpoint({ signal: AbortSignal.abort('stop') }), e => e.code === 'ERR_FSQLITE_JOB_CANCELLED');
  assert.equal(queue.stats.checkpointRecoveryRequired, true);
  assert.equal(worker.sent.filter(r => r.kind === 'checkpoint-recover').length, 0);
  await queue.recoverCheckpoint(); await queue.close();
});

test('malformed readback after a generic store failure fences publication until confirmation', async t => {
  const { db, worker } = await open(t); await worker.control('fault', 'store-ack'); await assert.rejects(db.checkpoint());
  await worker.control('fault', 'bad-envelope'); await assert.rejects(db.recoverCheckpoint(), receiptFailure);
  await assert.rejects(db.checkpoint(), receiptFailure);
  await db.recoverCheckpoint(); assert.equal((await worker.control('stats')).saves, 1); await db.close();
});

test('read-only recovery preserves retained prepared handles and SQL values', async t => {
  const { db, worker } = await open(t), statement = await db.prepare('SELECT ? AS value');
  await worker.control('fault', 'bad-receipt'); await assert.rejects(db.checkpoint());
  const before = (await worker.control('stats')).prepared;
  await db.recoverCheckpoint();
  assert.deepEqual((await statement.query(['still live'])).rowArrays, [['still live']]);
  assert.deepEqual((await worker.control('stats')).prepared, before);
  await statement.finalize(); await db.close();
});

test('queue wait timeout does not interrupt another active recovery or clear its fence early', async t => {
  const { db: queue, worker } = await open(t, true); await worker.control('fault', 'bad-receipt');
  await assert.rejects(queue.transaction(tx => tx.execute('INSERT INTO items VALUES(4)')), FrankenCheckpointCommitError);
  await worker.control('hold'); const held = worker.notice('confirm-held'), active = queue.recoverCheckpoint(); await held;
  await assert.rejects(queue.recoverCheckpoint({ waitTimeoutMs: 1 }), e => e.code === 'ERR_FSQLITE_JOB_WAIT_TIMEOUT');
  assert.equal(queue.stats.checkpointRecoveryRequired, true); assert.equal(queue.stats.activeJobs, 1);
  assert.equal(worker.sent.filter(r => r.kind === 'checkpoint-recover').length, 1);
  await worker.control('release'); await active; assert.equal(queue.stats.checkpointRecoveryRequired, false); await queue.close();
});

test('memory sessions and closed sessions cannot use snapshot recovery', async t => {
  const worker = transport(t), db = await FrankenDB.open({ worker });
  assert.equal(db.checkpointRecoverySupported, false);
  await assert.rejects(db.recoverCheckpoint(), e => e.code === 'ERR_FSQLITE_SNAPSHOT_RECOVERY_UNAVAILABLE'); await db.close();
  await assert.rejects(db.recoverCheckpoint());
  assert.equal(worker.sent.filter(r => r.kind === 'checkpoint-recover').length, 0);
});
