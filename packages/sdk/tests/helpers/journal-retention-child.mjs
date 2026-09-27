import { ChangesetRebaseJournal } from '../../src/changeset-rebase-journal.ts';
import { SqliteTarget } from './production-sqlite-target.mjs';

const [file, cut, encoded] = process.argv.slice(2);
const target = new SqliteTarget(file);
const journal = new ChangesetRebaseJournal(target, { journalId: 'source:conflicts' });
const pause = async () => {
  process.send({ cut });
  await new Promise(() => {});
};
target.after = async (kind, sql) => {
  if (kind !== 'execute') return;
  if ((cut === 'checkpoint' && sql.startsWith('INSERT OR ABORT INTO main."__fsqlite_rebase_journal_retention"')) ||
      (cut === 'delete' && sql.startsWith('DELETE FROM main."__fsqlite_rebase_journal_entries"'))) await pause();
};
if (cut === 'before-commit') target.beforeCommit = pause;
if (cut === 'after-commit') target.afterCommit = pause;
try {
  await journal.retireThrough(JSON.parse(encoded));
  throw new Error('Expected process interruption was not reached');
} finally { target.close(); }
