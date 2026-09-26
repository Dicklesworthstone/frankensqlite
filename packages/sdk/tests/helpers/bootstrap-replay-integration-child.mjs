import { SQLiteTarget } from './sqlite-integration-target.mjs';
import { ChangesetBootstrapReceiver } from '../../src/changeset-bootstrap.ts';
import { ChangesetBootstrapTransfer } from '../../src/changeset-bootstrap-transfer.ts';
const [sourcePath, receiverPath, journal] = process.argv.slice(2);
const source = new SQLiteTarget(sourcePath, 'UTF-16le', journal);
const destination = new SQLiteTarget(receiverPath, 'UTF-16le', journal);
let installed = false;
destination.onExecute = sql => { if (sql.includes('SET installed=1')) installed = true; };
destination.onCommit = () => { if (installed) process.kill(process.pid, 'SIGKILL'); };
const receiver = new ChangesetBootstrapReceiver(destination, {
  receiverId: 'replica', tables: ['notes'], confirmCommit: async () => {},
});
await new ChangesetBootstrapTransfer(source, {
  receiverId: 'replica', deliveryId: 'source:baseline', tables: ['notes'],
  transport: receiver, confirmSource: async () => {},
}).run();
throw Error('Expected process death after receiver COMMIT');
