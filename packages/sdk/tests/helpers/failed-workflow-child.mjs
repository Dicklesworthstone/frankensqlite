import { DurableJobQueue, DURABLE_JOBS_TABLE } from '../../src/durable-jobs.ts';
import { JobSqliteTarget } from './durable-jobs-sqlite-target.mjs';
const [path,cut]=process.argv.slice(2);
const db=new JobSqliteTarget(path);
const queue=await DurableJobQueue.open(db,'work',{clock:()=>100});
const stop=async()=>{process.send(cut);await new Promise(()=>{});};
let count=0;
db.afterSql=async sql=>{
  if(sql.startsWith(`UPDATE main."${DURABLE_JOBS_TABLE}" AS candidate`)) {
    count++;
    if((cut==='first-update'&&count===1)||(cut==='last-update'&&count===2))await stop();
  }
};
db.beforeCommit=async nested=>{if(!nested&&cut==='before-commit')await stop();};
db.afterCommit=async nested=>{if(!nested&&cut==='after-commit')await stop();};
await queue.cancelBlocked();
throw new Error('Requested failure-propagation boundary was not reached');
