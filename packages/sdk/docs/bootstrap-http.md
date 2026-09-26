# HTTP transport for atomic bootstrap

`createBootstrapHttpTransport` implements the `status`, `stage`, and `install`
transport used by `ChangesetBootstrapTransfer`. It sends the original retained
binary chunks and manifest, not a new snapshot, JSON row array, or ordinary
incremental changeset acknowledgement.

```ts
import {
  ChangesetBootstrapTransfer,
  createBootstrapHttpTransport,
} from '@frankensqlite/sdk';

const transport = createBootstrapHttpTransport('https://replica.example/bootstrap', {
  headers: async () => ({ authorization: `Bearer ${await getAccessToken()}` }),
  orderedSourceId: 'source-42:incarnation-1',
  maxChunkBytes: 1024 * 1024,
});
const transfer = new ChangesetBootstrapTransfer(source, {
  receiverId: 'replica-east',
  deliveryId: 'source-42:baseline-1',
  tables: ['parents', 'children'],
  orderedSourceId: 'source-42:incarnation-1',
  transport,
  confirmSource: () => source.checkpoint(),
});
await transfer.run({ maxChunks: 100, timeoutMs: 60_000 });
```

The endpoint must implement the bootstrap protocol below. The existing ordinary
and ordered incremental HTTP endpoints are deliberately incompatible. Staging
progress, including an `installed` status observation, is not a confirmed
installation receipt and cannot authorize source payload reclamation.

## Request and response contract

Every request is POST to the configured exact endpoint. Content-Type is
`application/vnd.fsqlite.bootstrap.v1`; Accept is
`application/vnd.fsqlite.bootstrap-response.v1+json`. The required
`x-fsqlite-bootstrap-action` header is `status`, `stage`, or `install`. It must
match the framed action, allowing endpoints to bound control requests without
admitting a chunk-sized body. This header is framing, not authentication.

The body is ASCII `FCB1`, a four-byte unsigned big-endian metadata length,
UTF-8 JSON metadata, and the exact chunk bytes. Metadata contains
`protocol: 'fsqlite-bootstrap-http-v1'`, `action`, `manifest`, and `byteLength`.
Only `stage` has an `index` and a binary remainder; control requests require a
zero byte length and no index. The embedded manifest is the existing
`fsqlite-bootstrap-v1` manifest with its ordered SHA-256 chain. Metadata is
limited to 128 KiB independently of chunk size. Names are captured and
ASCII-case-folded as in the source manifest API; table order is preserved.

HTTP 200 must contain JSON with the response media type, HTTP `protocol`,
`action`, manifest `sha256`, and `result`. A stage response also echoes its
`index`. Status returns null or bounded consistent progress. Stage returns a
prefix covering the submitted index. Install returns the complete existing
`BootstrapInstallReceipt`, with exact identity, totals and `confirmed: true`.
When `orderedSourceId` is configured, the receipt must contain that exact source
incarnation and the seed's terminal sequence. Otherwise ordered receipts reject;
there is no silent downgrade or enrollment from remote fields.

Responses are read incrementally with an 8 KiB actual-byte bound. Wrong media
types, malformed UTF-8/JSON, action/hash/index mismatch, inconsistent counters,
compressed bodies, dishonest lengths, and incomplete receipts reject. Returned
records are detached, frozen values. Ordinary incremental receipts cannot be
substituted for bootstrap responses.

## Credentials, errors and cancellation

Use HTTPS without userinfo or fragments. `allowInsecureLoopback` explicitly
permits development HTTP only at localhost, 127.0.0.1, or [::1]. Redirects reject,
including same-origin redirects. Requests omit ambient cookies, disable caching,
use no-referrer policy, and use CORS mode. The headers provider can supply explicit
credentials but cannot override framing, cookie, origin, host, or connection
headers. All input metadata and chunk bytes are captured before the provider
runs. Shared, resizable, detached, and oversized buffers reject.

`BootstrapHttpError.outcome` is `not-sent` before Fetch dispatch and `unknown`
afterwards. No post-dispatch failure proves receiver rollback. A lost install
response can follow a successful commit; retry the same retained manifest through
the transfer coordinator. The transport never retries automatically, discards
staged work, changes the source acknowledgement state, or generates another seed.
Error messages do not include endpoint URLs, credentials, or remote response
bodies. Local causes can contain sensitive host information; do not log them
indiscriminately.

The per-request timeout defaults to 30 seconds. A caller's remaining budget can
shorten it, never extend it. Cancellation covers credentials, Fetch, body reads
and cleanup. Started work is awaited, not abandoned with Promise.race; a trusted
custom Fetch or credentials provider that ignores cancellation can delay
settlement. The remote server may continue a commit after network cancellation.
Chunk bytes default to an 8 MiB bound, configurable through 64 MiB. Framing and
body reading materialize bounded buffers, not a zero-copy upload or a process-RSS
limit. Transport callbacks, TLS deployment, authenticated endpoint policy,
source/receiver storage confirmation and backup retention remain application
responsibilities. Hashes are integrity bindings, not credentials or consensus.

## Executed client verification

On Node 22.16.0 / SQLite 3.49.1, 86 tests pass with no failures or skips. The
client is exercised over real loopback HTTP against an independently implemented
native-SQLite staged endpoint. Native Session chunks preserve int64, NUL/Unicode
text and blobs; a lost install response is recovered without duplicate inserts.
Other tests cover exact response binding, ordered policy, redirects to a second
server, credential isolation, actual-byte bounds, stalled responses, cancellation
and input ownership. Protocol doubles cover malformed replies that ordinary HTTP
servers cannot produce. This is client/network interoperability evidence, not
production SDK receiver/store/fanout integration or Rust/WASM certification.

```sh
node --experimental-transform-types --test \
  packages/sdk/tests/changeset-bootstrap-http.test.mjs
```

Strict isolated TypeScript 5.8.3 checks pass against the pinned bootstrap and
transfer API declarations. This is not a full SDK build. Browser CORS execution,
production TLS/proxies, native MVCC, browser storage and physical power loss have
not been certified by these tests.

## Receiver endpoint

`createBootstrapHttpHandler` wraps an existing `ChangesetBootstrapReceiver` using
its `receiverId`, `status`, `stage`, and `install` methods. It does not create a
listener or database, provision schemas, or expose discard/reset remotely.

```ts
import {
  ChangesetBootstrapReceiver,
  createBootstrapHttpHandler,
} from '@frankensqlite/sdk';

const receiver = new ChangesetBootstrapReceiver(destination, {
  receiverId: 'replica-east',
  tables: ['parents', 'children'],
  orderedSourceId: 'source-42:incarnation-1',
  confirmCommit: () => destination.checkpoint(),
});
const handleBootstrap = createBootstrapHttpHandler(receiver, {
  authorize: request => authenticateBootstrapToken(
    request.headers.get('authorization'), request.receiverId, request.action,
  ),
  authorizeManifest: (request, info) => authorizeSourceNamespace(
    request.headers.get('authorization'), info.manifest.deliveryId, info.action,
  ),
  orderedSourceId: 'source-42:incarnation-1',
  allowedOrigins: ['https://app.example'],
  maxChunkBytes: 1024 * 1024,
  maxInFlight: 1,
});
// The application router passes a streaming Request and returns its Response.
// Authentication, namespace policy and storage confirmation above are app code.
```

Mandatory authorization must return literal `true`. It executes before a body
reader is acquired, with a detached Headers copy, fixed recipient and action,
URL, method and cancellation signal. It has no upload body or parsed manifest.
Optional manifest authorization runs after bounded parsing but before any
receiver operation, including status. Use it to bind an authenticated sender to
its source namespace and permitted actions. Receiver table admission and SQL
constraints remain enforced by the receiver, not invented by the HTTP layer.

The action header and frame must agree. Control bodies are bounded to metadata
plus eight framing bytes; they cannot use the stage payload allowance. All bodies
must match declared and actual lengths and use the correct uncompressed format.
The handler captures methods with their receiver binding, and awaits each call.
It validates and detaches the returned progress or full installation receipt
before serializing it. Staging is not checkpointed by this layer; the source
retains bytes until the underlying receiver confirms complete installation.
A completed status observation never substitutes for calling install again.

The handler's `orderedSourceId` must match the receiver's already provisioned
policy, and the client/transfer's matching trusted policy. The wire cannot add,
remove, or change that enrollment. Incorrect configuration fails closed; a
failed response is not evidence that an installation did not commit.

### Admission and cancellation

`maxInFlight` defaults to one and is capped at 64. It counts authorization, upload,
receiver execution, and awaited body cleanup. Overflow returns 503 without adding
a waiter or reading the rejected payload. An individual receiver may also refuse
concurrent calls; HTTP admission does not create a database pool or global writer
lock. A cancelled request retains its slot until already-started receiver work
and cleanup settle. SQL and storage confirmation are never abandoned with a timer
race. Callback/host implementations that ignore cancellation can delay settlement.

The host must preserve request streaming, propagate peer disconnects into
Request.signal, and correctly handle early body cancellation. Do not buffer the
entire upload in a router before invoking the handler. The application supplies
TLS, an exact route, trusted proxy/Host handling, header/connection limits and
process-level admission. Limits bound retained protocol bytes, not transport
buffers, arbitrary SQL work, total process RSS or physical durability.

Method/media failures return 405/415, authorization and CORS failures 403,
malformed input 400, byte limits 413, local timeout/cancellation 408, busy 503,
and receiver state conflicts 409. Other receiver/confirmation or invalid-result
failures return 500. Errors contain a constant redacted record with outcome
`unknown`, never SQL messages, credentials, routing IDs or stack traces. A client
may instead lose the connection. In every case it must reconcile the same seed,
not discard pending source bytes or manufacture a new delivery identity.

### Browser policy

Origins are exact HTTP(S) origins, with at most 64 configured additions. Same-
origin and originless requests still require authorization. Wildcards, opaque
origins, URL credentials and paths reject. OPTIONS permits only POST, does not
invoke authorization or SQL, and allows content-type, authorization, and the
bootstrap action header by default. Up to 32 additional exact header names may
be configured. Unlisted headers and ambient-cookie/connection headers reject.
Responses use exact-origin CORS, no credential opt-in, no-store, nosniff and Vary.
CORS does not authenticate non-browser callers. These header tests do not replace
an actual browser/TLS deployment test.

### Combined verification boundary

The combined suite passes 244 tests with zero failures or skips on Node 22.16.0 /
SQLite 3.49.1: 86 client tests, 82 handler tests, and 76 existing transfer-contract
tests. It executes the actual new HTTP client/handler and the existing production
transfer coordinator. The transfer loader explicitly substitutes the previously
published SQLite-backed source/store/receiver boundary fixtures; it does not load
the production bootstrap, outbox-store, or fanout modules. Native Session bytes,
real SQL transactions, real HTTP sockets and process deaths are exercised, but
these are network/coordination contracts, not full production-module integration.

Tests verify auth-before-read, manifest policy, CORS/preflight, malformed framing,
actual upload/response bounds, private-field method binding, overload, cancellation
and cleanup drain, redacted errors and exact ordered receipt admission. HTTP
transfer resumes a twelve-chunk baseline after a bounded run, retains source bytes
after a dropped install ACK, retries failed receiver/source confirmations, and
preserves slow-replica payloads under the fixture's fanout contract. Native SQL
constraint failure rolls back the fixture's whole baseline application.

Three separate HTTP receiver processes are SIGKILLed after staging, during native
row installation, and after COMMIT before its response. Fresh file-backed owners
resume the same seed. The original coordinator's three process-death cases also
run. This is process death, not power loss. No production SDK receiver/store/fanout,
full SDK/worker, Rust/WASM/MVCC, real browser storage or TLS deployment certification
is claimed. Strict isolated TypeScript 5.8.3 passes against the pinned public API
declaration fixtures, with exact optional properties, unchecked indexes and
noUnused checks. Existing SDK exports, native storage, concurrency defaults,
dependencies, workflows and beads are unchanged.

```sh
node --experimental-transform-types \
  --experimental-loader=./packages/sdk/tests/helpers/bootstrap-transfer-loader.mjs \
  --test packages/sdk/tests/changeset-bootstrap-http.test.mjs \
  packages/sdk/tests/changeset-bootstrap-http-handler.test.mjs \
  packages/sdk/tests/changeset-bootstrap-transfer-contract.test.mjs
```
