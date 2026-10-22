# Atomic ordered batches over HTTP

The explicit batch factories connect the existing source-bound batch transport
and atomic receiver to one binary HTTP POST. They do **not** loop over the
single-message endpoint. A server incapable of batch reception must reject the
request, not partially apply its prefix.

```ts
import {
  createOrderedChangesetBatchHttpHandler,
  createOrderedChangesetBatchHttpTransport,
  createOrderedChangesetBatchTransport,
} from '@frankensqlite/sdk';

// receiver is an explicitly provisioned OrderedChangesetReceiver. Its
// receiveBatch applies one outer SQL transaction and awaits one same-database
// confirmCommit barrier, including on replay. Never enroll from request data.
const handler = createOrderedChangesetBatchHttpHandler(receiver, {
  authorize: request => authenticateForReceiver(request),
  authorizeDelivery: (request, metadata) => authorizeSourceIdentity(request, metadata),
  maxMessageBytes: 8 * 1024 * 1024,
  maxBatchBytes: 32 * 1024 * 1024,
  maxInFlight: 1,
});
// Mount handler on an exact HTTPS route behind trusted host/proxy configuration.

const exchange = createOrderedChangesetBatchHttpTransport(endpointUrl, {
  headers: async (_metadata, signal) => ({
    Authorization: `Bearer ${await getToken(signal)}`,
  }),
  maxBatchBytes: 32 * 1024 * 1024,
});
const deliverBatch = createOrderedChangesetBatchTransport(sourceOutbox, {
  receiverId: receiver.receiverId,
  streamId: receiver.streamId,
  deliver: exchange,
});
// Supply deliverBatch to ChangesetDeliveryPump along with its ordinary single
// transport and confirmSource. Explicit runBatch then owns source ACK policy.
```

The snippet's authentication, token and routing functions are application-owned.
Identities and SHA-256 checks are not authentication. Credentials are omitted by
fetch unless supplied through the trusted header provider; redirects, cookies,
transport-header overrides and non-HTTPS remote endpoints remain disallowed.
An explicit development flag permits HTTP only on loopback hosts.

## Wire contract

Batch requests use `application/vnd.fsqlite.ordered-changeset-batch.v1` and
responses use `application/vnd.fsqlite.ordered-changeset-batch-receipt.v1+json`.
The request is exactly:

```
"FCB1" | uint32 big-endian metadata byte length | UTF-8 JSON metadata array |
raw changeset 0 | raw changeset 1 | ...
```

There are 1–256 metadata entries, each carrying the existing full-changeset
`protocol`, `receiverId`, `deliveryId`, `sha256`, `byteLength`, and `order` record.
Every sequence is a canonical positive signed-64-bit **decimal string**, not a
JSON number. All entries bind one receiver and one source incarnation, have
distinct delivery IDs, and form a contiguous sequence window. No bytes may
trail the declared payloads. Empty changesets remain real message identities;
an empty batch is invalid.

The response is an array of existing ordered confirmed receipts in the same
order and with exactly the same population. No receipt is returned to the
sender until every receipt validates. Source-owned counts and sequence evidence
are verified by `createOrderedChangesetBatchTransport`; use that wrapper rather
than treating this HTTP factory as an authoritative source ledger.

`FCD1` and `FCO1` single-message requests remain unchanged and are not accepted
on the batch endpoint. There is no content negotiation, fallback or automatic
retry. Cross-message foreign-key deferral is trusted local receiver policy,
not an incoming metadata field or HTTP option.

## Bounds, authorization and outcome

Per-message payload limits default to 8 MiB and cannot exceed 64 MiB. The batch
aggregate payload limit defaults to and cannot exceed 64 MiB. Metadata and
receipt arrays each have a separate 2,097,409-byte maximum (256 bounded 8 KiB
records plus array delimiters). Actual stream bytes are bounded regardless of
Content-Length; inconsistent lengths, unsupported encodings, truncated frames
and foreign media types reject. This is bounded whole-body ingestion, **not**
zero-copy or streaming SQL application. Private input/frame buffers and the
receiver's ownership copies also consume memory.

Request authentication runs before the handler pulls or buffers upload bytes.
It is required and fails closed. Optional per-delivery authorization runs for
all entries, in order, before the receiver is invoked once. Denial of a final
member cannot apply an earlier member. Authorization receives detached headers
and frozen metadata, not mutable changeset buffers.

The shared endpoint admission slot covers authentication, upload, application,
confirmation and cleanup. Cancellation does not release that slot while owned
receiver work can still settle. A client socket can disappear before the server
finishes; this does **not** prove rollback. Once fetch is dispatched, missing,
malformed or failed responses report an `unknown` outcome. A same-identity retry
recovers retained receipts and repeats confirmation without replaying SQL.

This layer does not acknowledge or reclaim source outbox payloads. The pump
retains its explicit omission-consent policy and per-identity source ACK and
confirmation rules. HTTP success depends on the configured receiver barrier;
a no-op memory barrier does not establish physical durability. Replay retention
must outlast the application's retry horizon.

## Qualification

The accompanying tests execute production receiver, ordering, application,
foreign-key and savepoint code against native reference SQLite, including real
loopback HTTP, authorization denial, cross-message constraints, journal BLOBs,
WAL reopen, truncated uploads, oversized responses and disconnects during held
confirmation. They are not FrankenSQLite Rust-engine, browser/CORS enforcement,
TLS deployment, full outbox/fanout-class or power-loss certification. No existing
HTTP tests are deleted or weakened; the partial qualification package separately
compares original and modified single-message framing/policy in one invocation.
