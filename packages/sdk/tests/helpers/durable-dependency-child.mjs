import { DurableJobQueue } from '../../src/durable-jobs.ts';
import { JobSqliteTarget } from './durable-jobs-sqlite-target.mjs';

const [path, cut] = process.argv.slice(2);
const db = new JobSqliteTarget(path);
const queue = await DurableJobQueue.open(db, 'q', { clock: () => 100 });
const stop = async () => { process.send({ cut }); await new Promise(() => {}); };
if (cut.startsWith('graph-')) {
  if (cut === 'graph-after-node') db.afterSql = async (sql, params) => {
    if (sql.startsWith('INSERT INTO main."__fsqlite_durable_jobs_v1"') && params[1] === 'b') await stop();
  };
  if (cut === 'graph-after-edge') db.afterSql = async sql => {
    if (sql.startsWith('INSERT INTO main."__fsqlite_job_dependencies_v1"')) await stop();
  };
  if (cut === 'graph-before-commit') db.beforeCommit = stop;
  if (cut === 'graph-after-commit') db.afterCommit = stop;
  await queue.enqueueBatch([
    { queue: 'q', id: 'join', payload: 'join', dependsOn: [{ queue: 'q', id: 'a' }, { queue: 'q', id: 'b' }] },
    { queue: 'q', id: 'b', payload: 'b' }, { queue: 'q', id: 'a', payload: 'a' },
  ]);
  throw new Error('Requested graph interruption point was not reached');
}
const firstEdge = (sql, params) => sql.startsWith('INSERT INTO main."__fsqlite_job_dependencies_v1"') && params[3] === 'a';
if (cut === 'before-edge') db.beforeSql = async (sql, params) => { if (firstEdge(sql, params)) await stop(); };
if (cut === 'after-edge') db.afterSql = async (sql, params) => { if (firstEdge(sql, params)) await stop(); };
if (cut === 'before-commit') db.beforeCommit = stop;
if (cut === 'after-commit') db.afterCommit = stop;
await queue.enqueueWith({ id: 'join', payload: 'joined', dependsOn: [{ queue: 'q', id: 'a' }, { queue: 'q', id: 'b' }] },
  tx => tx.execute("INSERT INTO effects VALUES(1,'once')"));
throw new Error('Requested interruption point was not reached');
