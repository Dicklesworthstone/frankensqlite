import { SqliteTarget } from './production-sqlite-target.mjs';
import { ChangesetRebaseJournal } from '../../src/changeset-rebase-journal.ts';

const config=JSON.parse(process.argv[2]);
const source=new SqliteTarget(config.path);
let deleting=false;
const kill=()=>new Promise(()=>{
  if(!process.send) throw new Error('Process-death evidence requires an IPC cut marker');
  process.send({cut:config.cut},()=>process.kill(process.pid,'SIGKILL'));
});
source.before=async(kind,sql)=>{
  if(kind==='execute' && sql.startsWith('DELETE FROM main."__fsqlite_rebase_journal_locals"')) {
    deleting=true;if(config.cut==='before-delete') await kill();
  }
};
source.after=async(kind,sql)=>{
  if(kind==='execute' && sql.startsWith('DELETE FROM main."__fsqlite_rebase_journal_locals"') && config.cut==='after-delete') await kill();
};
source.beforeCommit=async()=>{if(deleting && config.cut==='before-commit') await kill();};
source.afterCommit=async()=>{if(deleting && config.cut==='after-commit') await kill();};
await new ChangesetRebaseJournal(source,{journalId:config.journalId}).retireLocal('one',config.recordSha256);
throw new Error(`Requested process cut was not reached: ${config.cut}`);
