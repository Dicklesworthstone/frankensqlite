// Production HTTP factories, native Fetch streams and C SQLite session bytes.
// The SQL receiver below is an independent oracle, not the FrankenSQLite engine.
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { once } from "node:events";
import { createServer } from "node:http";
import { DatabaseSync } from "node:sqlite";
import { Readable } from "node:stream";
import { test } from "node:test";
import {
  createOrderedChangesetHttpTransport as transport,
  createOrderedChangesetHttpHandler as handler,
  createChangesetHttpTransport as legacyTransport,
  createChangesetHttpHandler as legacyHandler,
  CHANGESET_ORDERED_HTTP_CONTENT_TYPE as TYPE,
  CHANGESET_ORDERED_HTTP_RECEIPT_TYPE as RECEIPT,
  CHANGESET_HTTP_CONTENT_TYPE as LEGACY_TYPE,
  CHANGESET_HTTP_RECEIPT_TYPE as LEGACY_RECEIPT,
} from "../src/changeset-http.ts";

const url = "https://replica.test/ordered";
const hash = bytes => createHash("sha256").update(bytes).digest("hex");
const position = (sequence = "1", streamId = "source:incarnation-1") => ({
  protocol: "fsqlite-ordered-changeset-v1", streamId, sequence,
});
const message = (bytes = new Uint8Array(), sequence = "1") => ({
  protocol: "fsqlite-changeset-v1", receiverId: "replica", deliveryId: `source:${sequence}`,
  sha256: hash(bytes), changeset: bytes, order: position(sequence),
});
const ack = (m, extra = {}) => ({
  protocol: m.protocol, receiverId: m.receiverId, deliveryId: m.deliveryId,
  sha256: m.sha256, byteLength: m.changeset?.length ?? m.byteLength,
  applied: 0, omitted: 0, replayed: false, confirmed: true, order: m.order, ...extra,
});
const error = (code, outcome) => e => e.code === `ERR_FSQLITE_HTTP_${code}` && e.outcome === outcome;
function frame(m, tag = 79) {
  const { changeset, ...fields } = m;
  const meta = new TextEncoder().encode(JSON.stringify({ ...fields, byteLength: changeset.length }));
  const wire = new Uint8Array(8 + meta.length + changeset.length);
  wire.set([70, 67, tag, 49]);
  new DataView(wire.buffer).setUint32(4, meta.length);
  wire.set(meta, 8); wire.set(changeset, 8 + meta.length);
  return wire;
}
async function decode(request) {
  const bytes = new Uint8Array(await request.arrayBuffer());
  assert.deepEqual([...bytes.subarray(0, 4)], [70, 67, 79, 49]);
  const count = new DataView(bytes.buffer).getUint32(4);
  const meta = JSON.parse(new TextDecoder().decode(bytes.subarray(8, 8 + count)));
  return { ...meta, changeset: bytes.subarray(8 + count) };
}
const request = (m, options = {}) => new Request(url, {
  method: "POST", headers: { "content-type": TYPE }, body: frame(m), ...options,
});
const reply = (m, headers = {}) => new Response(JSON.stringify(m), {
  headers: { "content-type": RECEIPT, ...headers },
});
const receiver = (receive = async m => ack(m)) => ({
  receiverId: "replica", streamId: position().streamId, receive,
});
const gate = () => {
  let resolve;
  const promise = new Promise(r => { resolve = r; });
  return { promise, resolve };
};
const tick = () => new Promise(resolve => setTimeout(resolve, 5));

for (const sequence of ["1", "9007199254740993", "9223372036854775807"]) {
  test(`ordered transport and handler preserve canonical sequence ${sequence}`, async () => {
    const bytes = new Uint8Array([0, 255, 84, 0, 1]);
    let authorization, credentials, received;
    const endpoint = handler(receiver(async m => { received = m; return ack(m); }), {
      authorize: () => true,
      authorizeDelivery: (_context, meta) => { authorization = meta; return true; },
    });
    const send = transport(url, {
      headers: meta => { credentials = meta; return { authorization: "Bearer example" }; },
      fetch: async req => {
        assert.equal(req.headers.get("content-type"), TYPE);
        assert.equal(req.headers.get("accept"), RECEIPT);
        assert.equal(req.redirect, "error"); assert.equal(req.credentials, "omit");
        assert.equal(req.cache, "no-store"); assert.equal(req.referrerPolicy, "no-referrer");
        return endpoint(req);
      },
    });
    const m = message(bytes, sequence), result = await send(m);
    assert.deepEqual(result.order, m.order);
    assert.deepEqual(received.changeset, bytes);
    for (const meta of [received, authorization, credentials, result]) {
      assert.ok(Object.isFrozen(meta)); assert.ok(Object.isFrozen(meta.order));
      assert.deepEqual(meta.order, position(sequence));
    }
    assert.equal(authorization.changeset, undefined);
  });
}
for (const bad of [undefined, null, {}, { ...position(), sequence: 1 },
  ...["0", "01", "-1", "+1", "1.0", "1e1", " 1", "1\n", "9223372036854775808", "9".repeat(1000)]
    .map(sequence => ({ ...position(), sequence })),
  { ...position(), protocol: "fsqlite-changeset-v1" }, { ...position(), streamId: "" },
  { ...position(), streamId: "\ud800" }, { ...position(), streamId: "a\0b" },
]) {
  test(`invalid order is rejected before dispatch: ${JSON.stringify(bad)}`, async () => {
    let calls = 0;
    const send = transport(url, { fetch: async () => { calls++; throw Error("dispatch"); } });
    await assert.rejects(send({ ...message(), order: bad }), error("PROTOCOL", "not-sent"));
    assert.equal(calls, 0);
    const endpoint = handler(receiver(async () => { calls++; throw Error("receive"); }), { authorize: () => true });
    assert.equal((await endpoint(request({ ...message(), order: bad }))).status, 400);
    assert.equal(calls, 0);
  });
}
test("order fields are own data, not inherited claims or application getters", async () => {
  let getters = 0, sends = 0;
  const send = transport(url, { fetch: async () => { sends++; throw Error("send"); } });
  for (const order of [Object.create(position()), Object.defineProperty(position(), "sequence", {
    get() { getters++; return "1"; },
  })]) await assert.rejects(send({ ...message(), order }), error("PROTOCOL", "not-sent"));
  assert.equal(getters, 0); assert.equal(sends, 0);
});
test("asynchronous credentials cannot change the copied payload or order", async () => {
  const m = message(new Uint8Array([1, 0, 255]));
  const send = transport(url, {
    headers: async () => {
      m.order.sequence = "2"; m.order.streamId = "other"; m.deliveryId = "other";
      m.changeset.fill(8); await Promise.resolve(); return {};
    },
    fetch: async req => {
      const got = await decode(req);
      assert.deepEqual(got.order, position()); assert.equal(got.deliveryId, "source:1");
      assert.deepEqual([...got.changeset], [1, 0, 255]); return reply(ack(got));
    },
  });
  assert.deepEqual((await send(m)).order, position());
});
for (const bad of [undefined, position("2"), position("1", "other"),
  { ...position(), protocol: "legacy" }, { ...position(), sequence: 1 },
]) {
  test(`stripped or forged receipt order is uncertain, never acknowledged: ${JSON.stringify(bad)}`, async () => {
    const send = transport(url, { fetch: async req => reply(ack(await decode(req), { order: bad })) });
    await assert.rejects(send(message()), error("PROTOCOL", "unknown"));
    const endpoint = handler(receiver(async m => ack(m, { order: bad })), { authorize: () => true });
    assert.equal((await endpoint(request(message()))).status, 500);
  });
}
for (const extra of [{ receiverId: "wrong" }, { deliveryId: "wrong" }, { sha256: "f".repeat(64) },
  { byteLength: 1 }, { confirmed: false }, { applied: -1 }, { omitted: 100001 }, { replayed: 0 }]) {
  test(`base receipt binding also remains mandatory: ${JSON.stringify(extra)}`, async () => {
    await assert.rejects(transport(url, { fetch: async req => reply(ack(await decode(req), extra)) })(message()),
      error("PROTOCOL", "unknown"));
  });
}
test("legacy and ordered protocols cannot downgrade by media type, magic, or metadata", async () => {
  const plain = { ...message() }; delete plain.order;
  let calls = 0;
  const legacy = legacyHandler({ receiverId: "replica", receive: async m => {
    calls++; const r = ack(m); delete r.order; return r;
  } }, { authorize: () => true });
  const ordered = handler(receiver(async m => { calls++; return ack(m); }), { authorize: () => true });
  assert.equal((await ordered(new Request(url, { method: "POST", headers: { "content-type": LEGACY_TYPE }, body: frame(plain, 68) }))).status, 415);
  assert.equal((await legacy(request(message()))).status, 415);
  assert.equal((await ordered(request(message(), { body: frame(message(), 68) }))).status, 400);
  assert.equal((await legacy(request(message(), { headers: { "content-type": LEGACY_TYPE }, body: frame(message(), 68) }))).status, 400);
  assert.equal(calls, 0);
  await assert.rejects(legacyTransport(url, { fetch: legacy })(message(), {}), error("PROTOCOL", "not-sent"));
  await assert.rejects(transport(url, { fetch: async req => reply(ack(await decode(req)), { "content-type": LEGACY_RECEIPT }) })(message()),
    error("PROTOCOL", "unknown"));
  const ordinary = await legacyTransport(url, { fetch: legacy })(plain, {});
  assert.equal(ordinary.order, undefined); assert.equal(calls, 1);
});
test("authorization sees the trusted receiver and order, but never payload bytes", async () => {
  let received = 0, auth = 0;
  const endpoint = handler(receiver(async () => { received++; throw Error("receive"); }), {
    authorize: ctx => { auth++; assert.equal(ctx.receiverId, "replica"); return true; },
    authorizeDelivery: (_ctx, meta) => {
      assert.equal(meta.changeset, undefined); assert.deepEqual(meta.order, position()); return false;
    },
  });
  assert.equal((await endpoint(request(message()))).status, 403);
  assert.equal((await endpoint(request({ ...message(), order: position("1", "other") }))).status, 400);
  assert.equal(received, 0); assert.equal(auth, 2);
});
test("unauthorized uploads are cancelled before reading or allocating their payload", async () => {
  let pulled = 0, cancelled = 0, received = 0;
  const body = new ReadableStream({ pull() { pulled++; }, cancel() { cancelled++; } }, { highWaterMark: 0 });
  const endpoint = handler(receiver(async () => { received++; }), { authorize: () => false });
  const result = await endpoint(request(message(), { body, duplex: "half" }));
  assert.equal(result.status, 403); assert.equal(pulled, 0); assert.equal(cancelled, 1); assert.equal(received, 0);
});
test("one-byte stream chunks preserve framing; actual upload length remains bounded", async () => {
  let received = 0;
  const endpoint = handler(receiver(async m => { received++; return ack(m); }), { authorize: () => true, maxMessageBytes: 4 });
  const m = message(new Uint8Array([0, 255, 1, 2])), wire = frame(m);
  const body = new ReadableStream({ start(c) { for (const b of wire) c.enqueue(Uint8Array.of(b)); c.close(); } });
  assert.equal((await endpoint(request(m, { body, duplex: "half" }))).status, 200);
  assert.equal((await endpoint(request(message(new Uint8Array(5))))).status, 400);
  assert.equal((await endpoint(request(m, { headers: { "content-type": TYPE, "content-length": "999999" } }))).status, 413);
  assert.equal(received, 1);
});
for (const headers of [{ "content-length": "01" }, { "content-encoding": "gzip" }, { "content-length": "100000" }]) {
  test(`invalid receipt body headers cannot bypass admission: ${JSON.stringify(headers)}`, async () => {
    await assert.rejects(transport(url, { fetch: async req => reply(ack(await decode(req)), headers) })(message()),
      e => e.outcome === "unknown" && ["ERR_FSQLITE_HTTP_LIMIT", "ERR_FSQLITE_HTTP_PROTOCOL"].includes(e.code));
  });
}
test("oversized and truncated receipts are refused", async () => {
  for (const response of [new Response("x".repeat(8193), { headers: { "content-type": RECEIPT } }),
    new Response("{}", { headers: { "content-type": RECEIPT, "content-length": "3" } })]) {
    await assert.rejects(transport(url, { fetch: async () => response })(message()), e => e.outcome === "unknown");
  }
});
test("maximum escaped identities and 64-bit sequence fit bounded metadata and receipt", async () => {
  const m = message(new Uint8Array(), "9223372036854775807");
  m.receiverId = "\u0001".repeat(256); m.deliveryId = "\u0002".repeat(512); m.order.streamId = "\u0003".repeat(256);
  const endpoint = handler({ ...receiver(), receiverId: m.receiverId, streamId: m.order.streamId }, { authorize: () => true });
  assert.deepEqual((await transport(url, { fetch: endpoint })(m)).order, m.order);
});
test("CORS requires an exact allowed origin and never ambient cookie credentials", async () => {
  let calls = 0;
  const endpoint = handler(receiver(), { authorize: () => { calls++; return true; }, allowedOrigins: ["https://app.test"] });
  const preflight = origin => new Request(url, { method: "OPTIONS", headers: {
    origin, "access-control-request-method": "POST", "access-control-request-headers": "authorization, content-type",
  } });
  const allowed = await endpoint(preflight("https://app.test"));
  assert.equal(allowed.status, 204); assert.equal(allowed.headers.get("access-control-allow-origin"), "https://app.test");
  assert.equal(allowed.headers.get("access-control-allow-credentials"), null);
  assert.equal((await endpoint(preflight("https://evil.test"))).status, 403); assert.equal(calls, 0);
  const r = await endpoint(request(message(), { headers: { "content-type": TYPE, origin: "https://app.test" } }));
  assert.equal(r.headers.get("content-type"), RECEIPT); assert.equal(r.headers.get("cache-control"), "no-store");
});
test("pre-aborted sends never dispatch; cancellation during credentials is also not-sent", async () => {
  const c = new AbortController(); c.abort(); let sends = 0;
  const send = transport(url, { fetch: async () => { sends++; throw Error("send"); } });
  await assert.rejects(send(message(), { signal: c.signal }), error("CANCELLED", "not-sent"));
  const d = new AbortController();
  await assert.rejects(transport(url, { headers: () => { d.abort(); return {}; }, fetch: async () => { sends++; } })(message(), { signal: d.signal }),
    error("CANCELLED", "not-sent"));
  assert.equal(sends, 0);
});
test("cancelled server retains admission until in-flight receiver work settles", async () => {
  const started = gate(), finish = gate(); const c = new AbortController();
  let calls = 0;
  const endpoint = handler(receiver(async m => {
    calls++; started.resolve(); await finish.promise; return ack(m);
  }), { authorize: () => true });
  const pending = endpoint(request(message(), { signal: c.signal }));
  await started.promise; c.abort();
  assert.equal((await endpoint(request(message()))).status, 503);
  finish.resolve(); assert.equal((await pending).status, 408); assert.equal(calls, 1);
  assert.equal((await endpoint(request(message()))).status, 200);
});
test("timeouts drain custom fetch instead of resolving early with abandoned work", async () => {
  const started = gate(), finish = gate(); let settled = false;
  const pending = transport(url, { timeoutMs: 10, fetch: async req => {
    const m = await decode(req); started.resolve(); await finish.promise; return reply(ack(m));
  } })(message()).finally(() => { settled = true; });
  const rejection = assert.rejects(pending, error("TIMEOUT", "unknown"));
  await started.promise; await new Promise(r => setTimeout(r, 25)); assert.equal(settled, false);
  finish.resolve(); await rejection;
});
for (const code of ["ERR_FSQLITE_ORDERED_BUSY", "ERR_FSQLITE_ORDER_BUSY", "ERR_FSQLITE_ORDER_GAP"]) {
  test(`retryable ordering status ${code} preserves uncertainty and redacts internal errors`, async () => {
    const endpoint = handler(receiver(async () => { throw Object.assign(Error("private SQL credentials"), { code }); }), { authorize: () => true });
    const result = await endpoint(request(message()));
    assert.equal(result.status, code.endsWith("GAP") ? 409 : 503);
    assert.deepEqual(await result.json(), { error: "ERR_FSQLITE_HTTP_REJECTED", outcome: "unknown" });
  });
}

async function serve(t, endpoint) {
  const failures = [], pending = new Set();
  const server = createServer((req, res) => {
    const work = (async () => {
      const controller = new AbortController();
      const abort = () => { if (!res.writableEnded) controller.abort(); };
      req.once("aborted", abort); res.once("close", abort);
      try {
        const input = new Request(`http://127.0.0.1:${server.address().port}${req.url}`, {
          method: req.method, headers: req.headers, body: Readable.toWeb(req), duplex: "half", signal: controller.signal,
        });
        const output = await endpoint(input);
        res.writeHead(output.status, Object.fromEntries(output.headers));
        res.end(new Uint8Array(await output.arrayBuffer()));
      } finally { req.off("aborted", abort); res.off("close", abort); }
    })().catch(e => { failures.push(e); res.destroy(); });
    pending.add(work); void work.finally(() => pending.delete(work));
  });
  server.listen(0, "127.0.0.1"); await once(server, "listening");
  t.after(async () => {
    server.closeAllConnections(); await new Promise(r => server.close(r));
    await Promise.allSettled(pending); assert.deepEqual(failures, []);
  });
  return `http://127.0.0.1:${server.address().port}/ordered`;
}
test("actual HTTP + SQLite native session apply: gap refusal, committed response loss, historical retry", async t => {
  const src = new DatabaseSync(":memory:"), dst = new DatabaseSync(":memory:");
  t.after(() => { src.close(); dst.close(); });
  const schema = "CREATE TABLE notes(id INTEGER PRIMARY KEY, body TEXT, data BLOB)";
  src.exec(schema); dst.exec(schema);
  dst.exec("CREATE TABLE receipt(seq INTEGER PRIMARY KEY, id TEXT NOT NULL, digest TEXT NOT NULL)");
  const session = src.createSession();
  src.prepare("INSERT INTO notes VALUES (?,?,?)").run(9223372036854775807n, "a\0🌍", new Uint8Array([0, 255]));
  const bytes = session.changeset(); session.close();
  let confirms = 0, lose = true;
  const endpoint = handler(receiver(async m => {
    const seq = BigInt(m.order.sequence), tip = dst.prepare("SELECT coalesce(max(seq),0) AS n FROM receipt").get().n;
    if (seq > BigInt(tip) + 1n) throw Object.assign(Error("gap"), { code: "ERR_FSQLITE_ORDER_GAP" });
    const old = dst.prepare("SELECT id,digest FROM receipt WHERE seq=?").get(seq);
    if (old) { assert.equal(old.id, m.deliveryId); assert.equal(old.digest, m.sha256); }
    else {
      dst.exec("BEGIN");
      try {
        assert.equal(dst.applyChangeset(m.changeset), true);
        dst.prepare("INSERT INTO receipt VALUES(?,?,?)").run(seq, m.deliveryId, m.sha256);
        dst.exec("COMMIT");
      } catch (e) { dst.exec("ROLLBACK"); throw e; }
    }
    confirms++;
    if (lose) { lose = false; throw Error("lost after commit"); }
    return ack(m, { applied: 1, replayed: !!old });
  }), { authorize: ctx => ctx.headers.get("authorization") === "Bearer example" });
  const address = await serve(t, endpoint);
  const send = transport(address, { allowInsecureLoopback: true, headers: () => ({ authorization: "Bearer example" }) });
  await assert.rejects(send(message(bytes, "2")), e => e.status === 409 && e.outcome === "unknown");
  await assert.rejects(send(message(bytes)), e => e.status === 500 && e.outcome === "unknown");
  assert.equal(dst.prepare("SELECT count(*) AS n FROM notes").get().n, 1);
  assert.equal((await send(message(bytes))).replayed, true);
  assert.equal(confirms, 2);
  assert.deepEqual({ ...dst.prepare("SELECT CAST(id AS TEXT) AS id, hex(body) AS body, hex(data) AS data FROM notes").get() },
    { id: "9223372036854775807", body: "6100F09F8C8D", data: "00FF" });
});
