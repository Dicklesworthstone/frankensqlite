import { SqliteTarget } from './production-sqlite-target.mjs';
import { ChangesetOrder } from '../../src/changeset-order.ts';
const [path, cut] = process.argv.slice(2), target = new SqliteTarget(path);
const kill = () => process.kill(process.pid, 'SIGKILL');
target.after = async (kind, sql) => {
  if (kind !== 'execute') return;
  if (cut === 'anchor' && sql.startsWith('INSERT OR ABORT INTO main."__fsqlite_changeset_order_retention"')) kill();
  if (cut === 'delete' && sql.startsWith('DELETE FROM main."__fsqlite_changeset_order"')) kill();
};
target.beforeCommit = async () => { if (cut === 'before-commit') kill(); };
target.afterCommit = async () => { if (cut === 'after-commit') kill(); };
await new ChangesetOrder(target, { receiverId: 'receiver', sourceId: 'source:incarnation' }).retireBefore(4n);
throw new Error(`Process-death cut was not reached: ${cut}`);
