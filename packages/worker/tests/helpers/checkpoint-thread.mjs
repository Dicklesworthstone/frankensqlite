// Test-only Node worker bridge and SQLite/IndexedDB reference environment.
// The SDK client, host, admission, snapshot store and recovery path are production.
import { parentPort, workerData } from 'node:worker_threads';
import { sqliteSnapshotWorker } from './snapshot-sqlite-core.mjs';
import { installIndexedDbModel } from './indexeddb-model.mjs';
import { IndexedDbSnapshotStore } from '../../src/snapshot-store.ts';
import { responseTransferList } from '../../src/result-codec.ts';
const model = installIndexedDbModel(), fixture = sqliteSnapshotWorker();
let fault = workerData?.fault, hold, release, saves = 0, confirms = 0;
const originalSave = IndexedDbSnapshotStore.prototype.save;
const originalConfirm = IndexedDbSnapshotStore.prototype.confirmPublication;
IndexedDbSnapshotStore.prototype.save = async function(...args) {
  saves++;
  if (fault === 'quota') { fault = null; throw new DOMException('full', 'QuotaExceededError'); }
  const saved = await originalSave.apply(this, args);
  if (fault === 'store-ack') { fault = null; throw new Error('lost store acknowledgement after commit'); }
  return saved;
};
IndexedDbSnapshotStore.prototype.confirmPublication = async function(...args) {
  confirms++;
  if (hold) { parentPort.postMessage({ notice: 'confirm-held' }); await hold; }
  return originalConfirm.apply(this, args);
};
parentPort.on('message', request => {
  if (request.testControl) {
    try {
      let result;
      if (request.testControl === 'fault') fault = request.value;
      else if (request.testControl === 'hold') hold = new Promise(r => { release = r; });
      else if (request.testControl === 'release') { hold = null; release?.(); }
      else if (request.testControl === 'corrupt') {
        const state = [...model.databases.values()][0], head = state.values.get('head');
        if (request.value === 'bytes') new Uint8Array(head.bytes)[100] ^= 1;
        if (request.value === 'revision') head.revision = crypto.randomUUID();
        if (request.value === 'absent') state.values.clear();
      } else if (request.testControl === 'stats') result = { saves, confirms, events: fixture.events,
        requestQueue: fixture.host.requestQueue, prepared: fixture.host.preparedStatements,
        head: [...model.databases.values()][0]?.values.get('head') };
      else throw new Error('unknown test control');
      parentPort.postMessage({ testControl: request.testControl, id: request.id, result });
    } catch (error) { parentPort.postMessage({ testControl: request.testControl, id: request.id, error: String(error) }); }
    return;
  }
  void fixture.host.handle(request).then(response => {
    if (response.kind === 'ready' && workerData?.legacy) delete response.data.checkpointRecovery;
    if (response.kind === 'ready' && workerData?.capability !== undefined) response.data.checkpointRecovery = workerData.capability;
    if ((request.kind === 'checkpoint' || request.kind === 'checkpoint-recover') && response.kind === 'checkpoint-result') {
      // Fault injection acts on wire data, not the store's immutable receipt.
      response = structuredClone(response);
      const value = fault;
      if (value === 'bad-receipt') { fault = null; response.data.sha256 = 'not-a-hash'; }
      if (value === 'bad-envelope') { fault = null; response.data = null; }
      if (value === 'wrong-token') { fault = null; response.data.revision = crypto.randomUUID(); }
      if (value === 'wrong-parent') { fault = null; response.data.parentRevision = crypto.randomUUID(); }
      if (value === 'worker-ack') {
        fault = null; response = { kind: 'error', requestId: request.requestId,
          error: { code: 'ERR_FSQLITE_WORKER', message: 'acknowledgement failed after save' } };
      }
    }
    parentPort.postMessage(response, responseTransferList(response));
  }).catch(error => { throw error; });
});
