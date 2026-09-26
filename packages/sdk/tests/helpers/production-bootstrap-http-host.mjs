import { createServer } from 'node:http';
import { Readable } from 'node:stream';

// Test host only. Framing, authentication admission and receiver decisions run
// in the production createBootstrapHttpHandler, not this socket adapter.
export async function serveBootstrap(handler, afterResponse = async () => false) {
  const pending = new Set();
  let address;
  const server = createServer((incoming, outgoing) => {
    const controller = new AbortController();
    incoming.once('aborted', () => controller.abort(new Error('peer disconnected')));
    outgoing.once('close', () => { if (!outgoing.writableFinished) controller.abort(new Error('response disconnected')); });
    const work = (async () => {
      try {
        const request = new Request(`${address}${incoming.url}`, {
          method: incoming.method, headers: incoming.headers,
          body: Readable.toWeb(incoming), duplex: 'half', signal: controller.signal,
        });
        const response = await handler(request);
        if (await afterResponse(request, response)) { outgoing.destroy(); return; }
        outgoing.writeHead(response.status, Object.fromEntries(response.headers));
        outgoing.end(new Uint8Array(await response.arrayBuffer()));
      } catch (error) { outgoing.destroy(error); }
    })();
    pending.add(work); void work.finally(() => pending.delete(work));
  });
  await new Promise((resolve, reject) => { server.once('error', reject); server.listen(0, '127.0.0.1', resolve); });
  address = `http://127.0.0.1:${server.address().port}`;
  return {
    url: `${address}/bootstrap`,
    async close() {
      await Promise.all([...pending]);
      await new Promise(resolve => { server.close(resolve); server.closeIdleConnections(); });
    },
  };
}
