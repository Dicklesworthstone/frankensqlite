import { DurableJobQueue, DURABLE_JOBS_TABLE } from '../../src/durable-jobs.ts';
import { JobSqliteTarget } from './durable-jobs-sqlite-target.mjs';

const [path,raw,cut]=process.argv.slice(2), lease=JSON.parse(raw);
const db=new JobSqliteTarget(path);
const q=await DurableJobQueue.open(db,'parents',{clock:()=>100});
async function stop(){process.send({cut});await new Promise(()=>{});}
db.afterSql=async(sql,params)=>{
  if(sql.includes(`INSERT INTO ${DURABLE_JOBS_TABLE}`)&&
    ((cut==='first-child'&&params[1]==='first')||(cut==='last-child'&&params[1]==='second')))await stop();
  if(cut==='parent-completed'&&sql.includes("state = 'completed'"))await stop();
};
if(cut==='after-commit')db.afterCommit=stop;
await q.completeAndEnqueue(lease,[{queue:'next',id:'first',payload:'one',priority:7},
  {queue:'other',id:'second',payload:'two',availableAt:200}],tx=>tx.execute("INSERT INTO effects VALUES(1,'applied')"));
throw new Error('Did not reach requested interruption point');
