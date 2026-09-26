import { SqliteTarget } from './production-sqlite-target.mjs';
import { ChangesetBootstrapReceiver } from '../../src/changeset-bootstrap.ts';
import { ChangesetBootstrapTransfer } from '../../src/changeset-bootstrap-transfer.ts';

const config = JSON.parse(process.argv[2]);
const source = new SqliteTarget(config.sourcePath), destination = new SqliteTarget(config.destinationPath);
let staging = false, installed = false, acknowledged = false;
const kill = () => process.kill(process.pid, 'SIGKILL');
destination.after = async (kind, sql) => {
  if (kind !== 'execute') return;
  if (sql.startsWith('INSERT OR ABORT INTO main."__fsqlite_bootstrap_chunks"')) staging = true;
  if (sql.startsWith('UPDATE OR ABORT main."__fsqlite_bootstrap_state" SET installed=1')) installed = true;
  if (config.cut === 'application-row' && sql.startsWith('INSERT OR ABORT INTO main."t"')) kill();
};
destination.beforeCommit = async () => {
  if (config.cut === 'install-before-commit' && installed) kill();
};
destination.afterCommit = async () => {
  if (config.cut === 'stage-commit' && staging) kill();
  if (config.cut === 'install-after-commit' && installed) kill();
};
source.after = async (kind, sql) => {
  if (kind === 'execute' && sql.startsWith('UPDATE OR ABORT main."__fsqlite_changeset_outbox" SET acknowledged=1')) acknowledged = true;
};
source.beforeCommit = async () => { if (config.cut === 'ack-before-commit' && acknowledged) kill(); };
source.afterCommit = async () => { if (config.cut === 'ack-after-commit' && acknowledged) kill(); };
const receiver = new ChangesetBootstrapReceiver(destination, { receiverId: 'replica', tables: ['t'],
  ...(config.ordered ? { orderedSourceId: config.sourceId } : {}), confirmCommit: async () => {},
});
const transfer = new ChangesetBootstrapTransfer(source, { receiverId: 'replica', deliveryId: config.root, tables: ['t'],
  ...(config.ordered ? { orderedSourceId: config.sourceId } : {}), transport: receiver, confirmSource: async () => {},
});
await transfer.run();
throw new Error(`Requested process cut was not reached: ${config.cut}`);
