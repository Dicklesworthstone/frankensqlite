import { DurableJobQueue } from '../../src/durable-jobs.ts';
import { JobSqliteTarget } from './durable-jobs-sqlite-target.mjs';

const [path, cut] = process.argv.slice(2);
const db = new JobSqliteTarget(path);
const queue = await DurableJobQueue.open(db, 'q', { clock: () => 100 });
const stop = async () => { process.send({ cut }); await new Promise(() => {}); };
const firstEdge = (sql, params) => sql.startsWith('INSERT INTO main."__fsqlite_job_dependencies_v1"') && params[3] === 'a';
if (cut === 'before-edge') db.beforeSql = async (sql, params) => { if (firstEdge(sql, params)) await stop(); };
if (cut === 'after-edge') db.afterSql = async (sql, params) => { if (firstEdge(sql, params)) await stop(); };
if (cut === 'before-commit') db.beforeCommit = stop;
if (cut === 'after-commit') db.afterCommit = stop;
await queue.enqueueWith({ id: 'join', payload: 'joined', dependsOn: [{ queue: 'q', id: 'a' }, { queue: 'q', id: 'b' }] },
  tx => tx.execute("INSERT INTO effects VALUES(1,'once')"));
throw new Error('Requested interruption point was not reached');
