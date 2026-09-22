# Ordered changeset replication over HTTP

The ordered HTTP factories carry the existing source-order envelope through a
binary POST and return a receipt binding the same stream incarnation, sequence,
receiver, delivery identity, digest and payload length. No custom framing is
needed between `createOrderedChangesetTransport` and an ordered receiver.

```ts
import {
  ChangesetDeliveryPump, ChangesetOrder, applyChangeset,
  createOrderedChangesetReceiver, createOrderedChangesetTransport,
  createOrderedChangesetHttpHandler, createOrderedChangesetHttpTransport,
} from '@frankensqlite/sdk';

// Trusted provisioning: this database has the intended schema and baseline.
// A fresh stream begins at source sequence 1. Never initialize from a request.
const order = new ChangesetOrder(destination, {
  receiverId: 'east', sourceId: 'device-42:incarnation-7',
});
await order.initialize();
const receiver = await createOrderedChangesetReceiver(order, {
  apply: (target, message, controls) => applyChangeset(target, message.changeset, {
    tables: ['notes'], deliveryId: message.deliveryId, ...controls,
  }),
  confirmCommit: confirmDestinationCommit,
});
const handleRequest = createOrderedChangesetHttpHandler(receiver, {
  authorize: context => authenticateEastRequest(context.headers, context.signal),
  authorizeDelivery: (context, meta) => authorizeSource(context, meta.order?.streamId),
});
// Mount handleRequest on an exact HTTPS application route. The server adapter
// must propagate client disconnect to Request.signal and await handler completion.

const deliver = createOrderedChangesetTransport(sourceOutbox, {
  receiverId: 'east', streamId: 'device-42:incarnation-7',
  deliver: createOrderedChangesetHttpTransport('https://east.example/replication/ordered', {
    headers: async (_metadata, signal) => ({ authorization: await getBearerToken(signal) }),
  }),
});
const pump = new ChangesetDeliveryPump(sourceOutbox, {
  receiverId: 'east', deliver, confirmSource: confirmSourceCommit,
});
await pump.run();
```

`destination` and `sourceOutbox` retain their existing transaction/ownership
requirements. A receiver-bound fanout adapter can replace `sourceOutbox` for one
fixed member. Confirmation callbacks must establish the configured durability
boundary on the **same** databases. For snapshot persistence that requires an
explicit successful checkpoint/recovery; a memory no-op is not durability.
Application schema provisioning, authentication, source incarnation admission,
and a new replica's baseline remain trusted application responsibilities.

## Wire and downgrade rules

The request content type is `application/vnd.fsqlite.ordered-changeset.v1` and
the response type is `application/vnd.fsqlite.ordered-changeset-receipt.v1+json`.
The frame is `FCO1`, a four-byte unsigned big-endian JSON metadata length,
UTF-8 JSON metadata, then unmodified binary changeset bytes. Metadata retains
`protocol: 'fsqlite-changeset-v1'` and the existing identity/digest/length fields,
plus `order: { protocol: 'fsqlite-ordered-changeset-v1', streamId, sequence }`.
`sequence` is a canonical decimal string in 1..9223372036854775807, not a JSON
number. The successful JSON receipt retains the same order and existing counts.

There is no wire negotiation, automatic fallback or automatic retry. Ordered
factories reject legacy media types, `FCD1`, missing order, wrong incarnations,
noncanonical sequences and stripped/mismatched receipts. Legacy factories now
reject an explicit order field rather than silently discarding it; ordinary
unordered messages keep their existing wire format. Use a separately mounted
ordered endpoint during upgrades. Both ends must support it before switching.

Payloads default to 8 MiB and cannot exceed 64 MiB; metadata and receipts are
independently limited to 8192 bytes. Streamed bytes are counted even without an
honest Content-Length. These are wire/allocation admission limits, not RSS caps.
Order and payload are captured before awaiting credentials. Credential and
delivery-authorization callbacks receive frozen metadata, including frozen
`order` on this endpoint, never payload bytes. Stream IDs are routing and policy
inputs, not authentication. Use the mandatory authorization callback and HTTPS.

The common HTTP implementation retains explicit credential headers, no ambient
cookies, no redirects, no caches, exact-origin CORS, bounded active requests,
body cancellation and redacted errors. A source gap returns 409; receiver busy
returns 503. These responses still have an **unknown** delivery outcome: a
previous attempt may already have committed. An unsuccessful or lost response
must not release the outbox payload. Reconcile and retry the same identity,
bytes and sequence; retained ledger receipts avoid reapplying historical work.

Cancellation does not promise remote rollback. The server retains its admission
slot until the receiver/confirmation settles; it does not race SQL against a
timer. A custom Fetch implementation must honor Request policies. The client
awaits its in-flight call even when cancelled; remote work may outlive a network
failure, so durable idempotency remains necessary.

## Verification

```sh
node --experimental-transform-types --test packages/sdk/tests/changeset-ordered-http.test.mjs
```

The suite executes the production HTTP factories with native Fetch streams and
actual loopback HTTP. C SQLite session bytes test binary preservation and a
reference receiver's commit/lost-response/retry behavior. It also covers order
and base-receipt forgery, cross-protocol refusal, 64-bit boundaries, credential
capture, Unicode, CORS, body admission and cancellation/drain. This initial
suite does not certify the production SQL application/pump/fanout chain,
FrankenSQLite Rust/WASM, browser storage, TLS infrastructure or power loss.
