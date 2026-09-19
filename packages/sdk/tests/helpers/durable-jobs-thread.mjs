// Independent SQLite connection running the production durable-job module.
// Not a FrankenSQLite engine substitute or a browser-persistence adapter.
import { parentPort, workerData } from 'node:worker_threads';
import { DatabaseSync } from 'node:sqlite';
import { DurableJobQueue } from '../../src/durable-jobs.ts';
import { DurableJobWorker } from '../../src/durable-job-worker.ts';

const sql = new DatabaseSync(workerData.path);
sql.exec('PRAGMA busy_timeout = 5000');
let tail = Promise.resolve();
const database = {
  transaction(work) {
    // Serialize transactions on THIS connection, not across worker connections
    // or application handlers. Heartbeats and completion share this owner.
    const pending = tail.then(async () => {
      sql.exec('BEGIN IMMEDIATE');
      try {
        const result = await work({
          execute: async (statement, params = []) => Number(sql.prepare(statement).run(...params).changes),
          query: async (statement, params = []) => ({ rows: sql.prepare(statement).all(...params) }),
        });
        sql.exec('COMMIT');
        return result;
      } catch (error) {
        sql.exec('ROLLBACK');
        throw error;
      }
    });
    tail = pending.catch(() => {});
    return pending;
  },
};

async function run() {
  const jobs = await DurableJobQueue.open(database, 'work', { clock: () => 1000 });
  parentPort.postMessage({ type: 'ready' });
  if (workerData.barrier) Atomics.wait(new Int32Array(workerData.barrier), 0, 0);
  if (workerData.mode.startsWith('managed-')) {
    const runner = DurableJobWorker.start(jobs, async lease => {
      if (workerData.mode === 'managed-hold-claim') {
        parentPort.postMessage({ type: 'held', lease });
        await new Promise(resolve => parentPort.once('message', resolve));
      }
      return { result: 'done', apply: async tx => {
        await tx.execute('INSERT INTO effects(job_id, owner) VALUES (?,?)', [lease.id, lease.owner]);
        if (workerData.mode === 'managed-hold-effects') {
          parentPort.postMessage({ type: 'held', lease });
          await new Promise(resolve => parentPort.once('message', resolve));
        }
      } };
    }, { owner: workerData.owner, concurrency: workerData.mode === 'managed-consume' ? 2 : 1,
      stopWhenIdle: true, clock: () => 1000, leaseMs: 30_000 });
    await runner.done;
    return { type: 'result', stats: runner.stats };
  }
  if (workerData.mode === 'consume') {
    const ids = [];
    for (;;) {
      const leases = await jobs.claimBatch(workerData.owner, { limit: 8, leaseMs: 60_000 });
      if (leases.length === 0) return { type: 'result', ids };
      for (const lease of leases) {
        await jobs.completeWith(lease, async tx => {
          await tx.execute('INSERT INTO effects(job_id, owner) VALUES (?,?)', [lease.id, lease.owner]);
        }, 'done');
        ids.push(lease.id);
      }
    }
  }
  const lease = await jobs.claim(workerData.owner, 10);
  if (lease === null) throw new Error('Expected a job to hold');
  if (workerData.mode === 'hold-claim') {
    parentPort.postMessage({ type: 'held', lease });
    // Keep a live port while the parent forcibly terminates this worker.
    await new Promise(resolve => parentPort.once('message', resolve));
  } else if (workerData.mode === 'hold-effects') {
    await jobs.completeWith(lease, async tx => {
      await tx.execute('INSERT INTO effects(job_id, owner) VALUES (?,?)', [lease.id, lease.owner]);
      parentPort.postMessage({ type: 'held', lease });
      await new Promise(resolve => parentPort.once('message', resolve));
    }, 'must-not-commit');
  } else {
    throw new Error('Unknown worker scenario');
  }
}

try {
  const result = await run();
  sql.close();
  if (result) parentPort.postMessage(result);
} catch (error) {
  try { sql.close(); } catch { /* Preserve the original failure. */ }
  throw error;
}
