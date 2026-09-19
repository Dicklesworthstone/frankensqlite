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

## Fetch-standard receiver endpoint

`createChangesetHttpHandler` mounts an existing `ChangesetReceiver` on an
application-owned route. It does not create an HTTP listener, select a database,
provision certificates or invent an authentication policy.

```ts
import { ChangesetReceiver, createChangesetHttpHandler } from '@frankensqlite/sdk';

const receiver = new ChangesetReceiver(destination, {
  receiverId: 'replica-42',
  tables: ['notes', 'tags'],
  confirmCommit: () => destination.checkpoint(),
});
const handleChangesets = createChangesetHttpHandler(receiver, {
  authorize: request => authorizeReceiverToken(
    request.headers.get('authorization'), request.receiverId, request.signal,
  ),
  authorizeDelivery: (request, delivery) => authorizeSourceIdentity(
    request.headers.get('authorization'), delivery.deliveryId,
  ),
  allowedOrigins: ['https://app.example'],
  maxMessageBytes: 8 * 1024 * 1024,
  maxInFlight: 1,
  timeoutMs: 30_000,
});
// Your server/router forwards its streaming Request to handleChangesets and
// returns the resulting Response. The two authorization functions are your policy.
```

Authorization is mandatory and must return literal `true`. Missing credentials,
false/truthy non-boolean results and thrown authorization errors deny access.
The first callback runs before the handler acquires a body reader or invokes the
receiver. Its context contains a detached Headers copy, routing identity, URL,
method and cancellation signal, but no body. Optional `authorizeDelivery` runs
after bounded frame parsing and before SQL; it can bind a source-qualified
operation ID to the authenticated sender's namespace. Configure the receiver's
fixed table allowlist as well. Identity strings and hashes are not credentials.
The application must trust its authorization callbacks and target schema; SQL
triggers/foreign-key effects are still governed by the receiver's SQL contract.

The endpoint accepts POST with the exact request media type. It enforces both
actual body bytes and declared lengths, validates framing/routing metadata, and
passes the owned binary payload to the real receiver. The receiver performs
codec/hash validation, transactional application and retained-receipt handling.
No HTTP 200 is constructed until its commit-confirmation callback has completed.
An unconfirmed or mismatched return value is rejected, including on replay.

### Admission, cancellation and errors

`maxInFlight` counts authorization, upload, policy checking and receiver execution
together, not only active SQL. It defaults to one and permits at most 64. Excess
requests receive 503 without entering an application queue. One ordinary
ChangesetReceiver still admits one call; increasing HTTP admission does not
create a database pool or override receiver ownership. This is per-handler
backpressure, not a global writer lock or a change to Rust MVCC defaults.

The slot remains held until started receiver work and body cleanup settle,
including after client cancellation. The handler forwards Request cancellation
and the remaining server-side monotonic budget to the receiver. It never races
away from SQL or storage confirmation. A trusted authorization callback, receiver
or host stream that ignores cancellation can delay settlement; it cannot create
an unbounded waiting queue inside this handler.

Unauthorized/rejected bodies are cancelled rather than read into application
buffers. The HTTP host must preserve streaming, propagate peer disconnects into
Request.signal, and handle early body cancellation correctly. Do not eagerly
buffer uploads before invoking the handler. TLS termination, trusted Host/proxy
handling, connection/header limits and process-level admission remain host
responsibilities. Body limits bound retained protocol bytes, not SQL memory,
network-stack buffering or total process RSS.

Method/media errors return 405/415; authorization/CORS failures return 403;
malformed/oversized wire input returns 400/413; local cancellation/deadline
expiry can return 408; admission/receiver busy returns 503; receiver execution,
confirmation or receipt validation failure returns 500. A disconnected client
may observe no status at all. These responses use a constant, redacted error
record and never include SQL errors, credentials, routing IDs or stack traces.
They do not prove rollback. The client treats every post-dispatch error as an
unknown outcome and keeps the original identity for reconciliation.

### Browser origins

Originless and same-origin requests still require authorization. Cross-origin
browser calls require an exact entry in `allowedOrigins` (maximum 64). Wildcards,
opaque/null origins, credentials and origin URLs with paths are rejected. A valid
OPTIONS preflight does not invoke authorization or SQL: it merely permits a
subsequent authenticated POST. Allowed preflight headers default to `content-type`
and `authorization`; up to 32 additional exact names can be configured with
`allowedHeaders`. Unlisted names/methods reject. Responses never opt into cookies
with Access-Control-Allow-Credentials, never use wildcard origins, and include
no-store, nosniff and appropriate Vary headers. CORS is not authentication, and
an originless non-browser client must not gain authority merely by omitting Origin.

## Combined executed verification

On Node 22.16.0 / SQLite 3.49.1, the complete suite passes **112 tests**, with no
failures or skipped tests. It exercises the actual existing capture, outbox,
pump, receiver, application and codec modules over real loopback HTTP, alongside
adversarial stream/protocol tests. All six changeset modules (including HTTP)
also pass strict TypeScript 5.8.3 checks with exact optional properties and
unchecked-index checking. This does not constitute a full SDK/worker build.

The complete-path test drops the first successful HTTP acknowledgement, verifies
that both ordered source deliveries remain pending, retries without rerunning
source callbacks, and obtains the correct final receiver rows and acknowledged
outbox state. A separate HTTP receiver child is actually SIGKILLed after its
SQLite commit but before returning a response. Another connection reopens the
file, receives the same delivery over HTTP, and returns its retained receipt
without inserting the row again. Constraint failures roll back earlier rows and
the inbox; failed confirmation returns no success and is retried on replay.

These are Node HTTP, reference-SQLite and process-death results. Browser CORS
headers are tested through standards-based Request/Response objects, not a real
browser. Production TLS/proxy deployments, Rust/WASM, browser persistent stores,
physical power loss, consensus and the native RaptorQ protocol are not certified.
