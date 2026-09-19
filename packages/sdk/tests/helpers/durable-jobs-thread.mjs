// Independent SQLite connection running the production durable-job module.
// Not a FrankenSQLite engine substitute or a browser-persistence adapter.
import { parentPort, workerData } from 'node:worker_threads';
import { DatabaseSync } from 'node:sqlite';
import { DurableJobQueue } from '../../src/durable-jobs.ts';

const sql = new DatabaseSync(workerData.path);
sql.exec('PRAGMA busy_timeout = 5000');
const database = {
  async transaction(work) {
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
  },
};

async function run() {
  const jobs = await DurableJobQueue.open(database, 'work', { clock: () => 1000 });
  parentPort.postMessage({ type: 'ready' });
  if (workerData.barrier) Atomics.wait(new Int32Array(workerData.barrier), 0, 0);
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
