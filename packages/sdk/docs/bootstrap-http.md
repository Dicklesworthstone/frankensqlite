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
