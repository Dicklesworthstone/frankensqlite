import { SqliteTarget } from './production-sqlite-target.mjs';
import { ChangesetRebaseJournal } from '../../src/changeset-rebase-journal.ts';
const [file, cut] = process.argv.slice(2);
const source = new SqliteTarget(file);
const journal = new ChangesetRebaseJournal(source, { journalId: 'source:conflicts' });
const stop = async () => {
  await new Promise((resolve, reject) => process.send({ cut }, error => error ? reject(error) : resolve()));
  process.kill(process.pid, 'SIGKILL');
  await new Promise(() => {});
};
const insertion = (kind, sql) => kind === 'execute' && sql.startsWith('INSERT OR ABORT INTO main."__fsqlite_changeset_outbox"');
if (cut === 'before-insert') source.before = async (kind, sql) => { if (insertion(kind, sql)) await stop(); };
if (cut === 'after-insert') source.after = async (kind, sql) => { if (insertion(kind, sql)) await stop(); };
if (cut === 'before-commit') source.beforeCommit = stop;
if (cut === 'after-commit') source.afterCommit = stop;
await journal.enqueueLocal('edit-1', { tables: ['t'] });
throw new Error('Requested process-death cut was not reached');
