import { writeSync } from 'node:fs';
import { ChangesetBootstrapReceiver } from '../../src/changeset-bootstrap.ts';
import { SqliteTarget } from './production-sqlite-target.mjs';

const [path, cut] = process.argv.slice(2);
const db = new SqliteTarget(path);
const manifest = JSON.parse(db.rows('SELECT manifest FROM __fsqlite_bootstrap_state WHERE id=1')[0][0]);
function stop() { writeSync(1, `CUT:${cut}\n`); process.kill(process.pid, 'SIGKILL'); }
if (cut === 'first-row') db.after = (_, sql) => { if (sql.startsWith('INSERT OR ABORT INTO main."child"')) stop(); };
if (cut === 'before-commit') db.beforeCommit = stop;
if (cut === 'after-commit') db.afterCommit = stop;
const receiver = new ChangesetBootstrapReceiver(db, {
  receiverId: 'replica', tables: ['child', 'parent'], foreignKeys: 'defer',
  confirmCommit: async () => { if (cut === 'confirmation') stop(); },
});
await receiver.install(manifest);
throw new Error('Expected process-death boundary was not reached');
