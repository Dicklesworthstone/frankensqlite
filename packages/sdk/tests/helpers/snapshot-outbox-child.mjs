import { SqliteTarget } from './production-sqlite-target.mjs';
import { ChangesetOutbox } from '../../src/changeset-outbox.ts';
const [path,phase]=process.argv.slice(2);
const source=new SqliteTarget(path),outbox=new ChangesetOutbox(source);
const stop=async()=>{process.send({phase});await new Promise(()=>{});};
if(phase==='after-payload')source.after=async(kind,sql)=>{if(kind==='execute'&&sql.startsWith('INSERT OR ABORT INTO main."__fsqlite_changeset_outbox"'))await stop();};
if(phase==='before-commit')source.beforeCommit=stop;
if(phase==='after-commit')source.afterCommit=stop;
await outbox.recordSnapshot(async tx=>{
  await tx.execute("UPDATE t SET v='paid'");
  if(phase==='after-work')await stop();
},{deliveryId:'source:trigger-operation',tables:['t','audit']});
throw new Error('kill point was not reached');
