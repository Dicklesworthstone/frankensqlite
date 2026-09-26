// HTTP process-death fixture: real native SQLite rows with the explicit transfer
// boundary receiver, not the production SDK bootstrap storage implementation.
import { createServer } from 'node:http';
import { Readable } from 'node:stream';
import { Receiver } from './bootstrap-transfer-fixture.mjs';
import { createBootstrapHttpHandler } from '../../src/changeset-bootstrap-http.ts';
const receiver = new Receiver({ depth: 0 }, { filename: process.env.BOOTSTRAP_HTTP_DB,
  initialize: process.env.BOOTSTRAP_HTTP_REOPEN !== '1' });
receiver.receiverId = 'east';
const cut = process.env.BOOTSTRAP_HTTP_CUT;
const die = () => process.kill(process.pid, 'SIGKILL');
if (cut === 'stage') receiver.hooks.afterStage = (_m, _i, result) => { die(); return result; };
if (cut === 'row') receiver.hooks.afterRow = die;
if (cut === 'commit') receiver.hooks.afterInstall = result => { die(); return result; };
const handler = createBootstrapHttpHandler(receiver, { authorize: c => c.headers.get('authorization') === 'Bearer test' });
const server = createServer(async (req, res) => {
  const controller = new AbortController();
  req.on('aborted', () => controller.abort());
  res.on('close', () => { if (!res.writableEnded) controller.abort(); });
  try {
    const request = new Request(`http://127.0.0.1:${server.address().port}${req.url}`, {
      method: req.method, headers: req.headers, body: Readable.toWeb(req), duplex: 'half', signal: controller.signal,
    });
    const response = await handler(request);
    if (!res.destroyed) { res.writeHead(response.status, Object.fromEntries(response.headers)); res.end(Buffer.from(await response.arrayBuffer())); }
  } catch { if (!res.destroyed) res.destroy(); }
});
server.listen(0, '127.0.0.1', () => process.send?.({ port: server.address().port }));
