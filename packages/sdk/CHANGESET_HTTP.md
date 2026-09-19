# Binary HTTP changeset delivery

`createChangesetHttpTransport` supplies the existing `ChangesetDeliveryPump` with
an actual Fetch-based transport. It sends retained binary changesets unchanged,
not SQL text, JSON arrays of bytes, or a new database format.

```ts
import { ChangesetDeliveryPump, createChangesetHttpTransport } from '@frankensqlite/sdk';

const deliver = createChangesetHttpTransport('https://replica.example/changesets', {
  headers: async () => ({ authorization: `Bearer ${await getAccessToken()}` }),
  maxMessageBytes: 8 * 1024 * 1024,
  timeoutMs: 30_000,
});
const pump = new ChangesetDeliveryPump(outbox, {
  receiverId: 'replica-42',
  confirmSource: () => source.checkpoint(),
  deliver,
});
await pump.run({ maxDeliveries: 100, timeoutMs: 60_000 });
```

The endpoint must implement the wire contract below. This is not a generic JSON
REST client. Applications own endpoint routing, authentication, TLS deployment,
source/receiver confirmation, retry pacing and the fixed outbox destination.
The transport never starts background work or automatically repeats a request.

## Transport and outcome contract

URLs must be absolute HTTPS, without userinfo or fragments. Development tests
may explicitly enable `allowInsecureLoopback` for HTTP at `localhost`,
`127.0.0.1` or `[::1]` only. Production traffic must use authenticated HTTPS.
Redirects are rejected, including same-origin redirects. Requests use
`credentials: 'omit'`, `cache: 'no-store'`, `referrerPolicy: 'no-referrer'` and
CORS mode. Ambient cookies are not an authentication mechanism for this API.
The optional asynchronous headers provider can supply explicit credentials;
it receives frozen routing metadata, not payload bytes. It cannot override
framing, cookie, origin, host or connection headers. It must be trusted and
respect cancellation. A custom Fetch implementation is similarly trusted and
must enforce the Request's policies; interception by application code or a
service worker is not prevented by this module.

The complete message is captured before invoking credentials or yielding.
Shared, resizable and detached input buffers reject. Message payload size
defaults to 8 MiB, configurable through 64 MiB. The transport owns a framed copy;
this is bounded materialization, not zero-copy or streaming upload.

Only HTTP 200 with the exact receipt media type can succeed. The receipt must
confirm the protocol, recipient, delivery ID, SHA-256 and byte length. Decision
counts and replay flags are validated; the pump additionally checks counts
against the decoded changeset and enforces its omission policy. The receiver
still validates the payload digest and actual SQL input. An HTTP status alone,
unconfirmed receipt, mismatched identity or malformed body cannot authorize
source acknowledgement.

Receipts are read incrementally with an 8 KiB actual-byte limit, regardless of
Content-Length. Oversized, truncated, invalid-UTF-8 or compressed responses
reject. Failed reads cancel/release their body readers. Remote response bodies,
SQL messages and credentials are not incorporated into transport error messages.
Opaque local causes remain available to the application and should not be logged
indiscriminately.

`ChangesetHttpError.outcome` is `not-sent` before Fetch dispatch and `unknown`
after it. **No error after dispatch proves rollback**, including timeouts, HTTP
errors, invalid receipts and lost responses. Retry only with the same retained
delivery ID and payload through the existing inbox/outbox protocol. The server
may finish committing after the network request is cancelled. Never use a new
identity to work around an uncertain response.

The total request budget defaults to 30 seconds. A pump's remaining timeout can
shorten, never extend, the configured request timeout. It includes credentials,
request/response transfer and receipt parsing. Abort is forwarded to Fetch and
body readers. The transport waits for started work/cleanup to settle rather than
abandoning an unobserved promise. A custom credential provider, stream or Fetch
implementation that ignores cancellation can therefore delay settlement.

## Version-one wire format

POST with Content-Type `application/vnd.fsqlite.changeset.v1` and Accept
`application/vnd.fsqlite.changeset-receipt.v1+json`.

The body consists of ASCII `FCD1`, a four-byte big-endian unsigned JSON length,
that many UTF-8 JSON bytes, then the exact binary SQLite changeset. JSON metadata
contains `protocol: 'fsqlite-changeset-v1'`, `receiverId`, `deliveryId`, `sha256`
(lowercase hexadecimal), and `byteLength`. Metadata is capped at 8 KiB; receiver
and delivery identities are respectively capped at 256 and 512 UTF-8 bytes.
The binary remainder must have exactly `byteLength` bytes, without trailing data.
An empty changeset is valid. No compression/content encoding is negotiated.

A successful receipt is bounded UTF-8 JSON with the receipt media type and
`protocol`, `receiverId`, `deliveryId`, `sha256`, `byteLength`, `confirmed: true`,
`applied`, `omitted` and `replayed`. Confirmation must represent the receiver's
actual storage boundary, including on a replay. This frame is routing/integrity
metadata, not a signature, credential, consensus log or multicast protocol.

## Executed client tests

The client suite passes 54 tests on Node 22.16.0, using real loopback HTTP/Fetch.
An independent SQLite 3.49.1 endpoint applies native session bytes and retains
an inbox decision, preserving int64, NUL/Unicode text and binary data. Tests also
cover redirects to a second server, explicit credentials, malformed/oversized
chunked receipts, cancellation during slow/stalled responses and unknown-outcome
retry identity. Deliberate protocol/transport doubles cover malformed responses
that a conforming HTTP server cannot produce.

```sh
node --experimental-loader=./packages/sdk/tests/helpers/source-loader.mjs \
  --test packages/sdk/tests/changeset-http.test.mjs
```

This is client/network and reference-SQLite evidence, not FrankenSQLite engine,
Rust/WASM, browser CORS or physical power-loss certification.
