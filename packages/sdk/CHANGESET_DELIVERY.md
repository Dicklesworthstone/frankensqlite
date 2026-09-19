# Confirmed changeset delivery

`ChangesetReceiver` connects a validated delivery envelope to the existing
`applyChangeset` SQL/receipt path. It does not treat a successful SQL call as an
implicit durable network acknowledgement.

```ts
import { ChangesetReceiver } from '@frankensqlite/sdk';

const receiver = new ChangesetReceiver(destination, {
  receiverId: 'replica-A',
  tables: ['notes', 'tags'],
  // For a snapshot-backed FrankenDB, publish rows AND inbox before an ACK.
  confirmCommit: () => destination.checkpoint(),
});

// Authenticate and decode the transport request before invoking this method.
const receipt = await receiver.receive(envelope, { signal, timeoutMs: 10_000 });
// Only a fulfilled receive() may become a successful transport response.
```

An envelope has own data properties `protocol: 'fsqlite-changeset-v1'`,
`receiverId`, `deliveryId`, `sha256`, and `changeset: Uint8Array`. Receiver IDs
are routing identities, not credentials. Delivery IDs must be globally
source-qualified, valid UTF-8 of 1..512 bytes without NUL, and must never be
reused for a different operation. The application owns transport framing,
authentication, authorization and source identity; neither SHA-256 nor a receiver
name authenticates a sender. No fetch, socket, credential handling or background
task is installed by this helper.

The receiver captures bounded, owned bytes before yielding, verifies their
SHA-256 and SQLite changeset structure, and applies them through one owned SQL
transaction with an atomic retained receipt. Shared, resizable and detached
buffers reject; typed-array subclass iterators/getters cannot bypass the copy
budget. `maxMessageBytes` defaults to 8 MiB and has a hard 64 MiB maximum. Existing
codec row/cell/table limits still apply. This is bounded materialization, not
streaming or a limit on all SQL execution memory.

## SQL commit is not the acknowledgement boundary

`confirmCommit` is mandatory, is awaited AFTER the owned transaction returns,
and must confirm the SAME top-level database containing application rows and
`__fsqlite_changeset_receipts`. In snapshot modes it must checkpoint or resolve
an uncertain checkpoint through the existing recovery API. It must reject while
publication remains unknown. A durable file-backed SQL adapter may explicitly
use `async () => {}` when its COMMIT already establishes the required durability.
An in-memory target with a no-op barrier is still only in-memory delivery.

The helper cannot verify arbitrary application-supplied confirmation code or
certify storage hardware. The returned `confirmed: true` means that the configured
barrier completed; its durability is only as strong as that target and barrier.
Use a top-level privately owned connection, not a provisional enclosing
transaction. Do not let unrelated SQL replace receipt state between commit and
confirmation. A real authenticated remote ACK is the transport's responsibility.

The receipt binds protocol, receiver identity, delivery ID, SHA-256 and byte
length to the retained `applied`, `omitted` and `replayed` result. A checkpoint
failure or lost response does not roll back already committed SQL. Retry the
SAME ID and exact bytes after resolving the storage failure: the retained
receipt avoids repeating application writes. **Replay also runs confirmation**;
a receipt present only in memory must not authorize source payload reclamation.

Omission is an explicit local `onConflict` decision using applyChangeset's
existing policy. Constraints always fail. The captured table allowlist is
checked on replay too; SDK/system tables cannot be direct delivery targets.
Receipt retention, trigger/FK behavior and the documented limitations of
`applyChangeset` remain unchanged. Retiring receipts or restoring an old backup
also retires the corresponding deduplication history.

## Cancellation and lifecycle

Only one receive may be active on an instance. Overlap rejects without building
an unbounded queue; this is local connection admission, not global writer
serialization or a distributed receiver lock. Independent instances still rely
on their database's transaction/conflict rules.

Cancellation and monotonic deadlines are cooperative. They prevent subsequent
SQL/confirmation work, propagate into applyChangeset's SQL checkpoints, and wait
for an already-started transaction or confirmation callback to settle. They do
not abandon commits, race promises, interrupt arbitrary synchronous code, or
promise a hard wall-clock completion bound. A cancellation after SQL commit can
leave a receipt for a later replay. Errors retain their cause, delivery ID and
phase (`validate`, `receiver-apply`, or `receiver-confirm`), rather than claiming
that every rejection proves rollback.

## Executable reference tests

```sh
node --experimental-loader=./packages/sdk/tests/helpers/source-loader.mjs \
  --test packages/sdk/tests/changeset-delivery.test.mjs
```

The initial receiver suite passed 29/29 tests on Node 22.16.0 / SQLite 3.49.1.
Tests execute real SQLite SQL/transactions, native session output, independent
process reopen, rollback, deferred commit failure, lost commit/receiver responses,
failed confirmation, replay, input ownership, overlap and cancellation. They also
starve timer tasks to verify monotonic SQL deadline checks. Strict TypeScript
5.8.3 checks passed. These are SDK helper reference tests, not FrankenSQLite
Rust/WASM, browser persistence, full SDK build or power-loss certification.

## Ordered outbox delivery

`ChangesetDeliveryPump` connects the existing `ChangesetOutbox` to a receiver,
including source confirmation, strict ACK validation and recoverable source
acknowledgement. Each `run` is bounded and awaited; nothing is scheduled in the
background and no source transaction is held across transport.

```ts
import { ChangesetDeliveryPump } from '@frankensqlite/sdk';

// source and destination are separate, top-level snapshot-backed databases.
// outbox was constructed on this exact source; receiver on this destination.
const pump = new ChangesetDeliveryPump(outbox, {
  receiverId: receiver.receiverId,
  confirmSource: () => source.checkpoint(),
  // A remote implementation must authenticate the endpoint and response.
  deliver: (message, controls) => receiver.receive(message, controls),
});

const result = await pump.run({
  maxDeliveries: 100,
  maxBytes: 64 * 1024 * 1024,
  signal,
  timeoutMs: 30_000,
});
```

A transport callback may cross a process/network boundary, but must preserve the
protocol fields and owned payload and return the receiver's full receipt, not
an HTTP status, boolean, arbitrary ID, or unverified digest. In-memory direct
calls are useful too, but do not become durable replication just because the
helper is named a delivery pump. As with receiver confirmation, an explicitly
durable SQL source can supply an async no-op instead of snapshot checkpointing.

### The acknowledgement sequence

A run first confirms source state, **even if the outbox appears empty**. An
acknowledgement left only in memory by an earlier failed checkpoint must be
resolved before that empty list can be reported as complete.

The pump then selects the oldest pending entry, loads and verifies exactly one
payload, and confirms source state again before sending. This second barrier
covers source writes committed between the initial confirmation and the read.
It submits the envelope, verifies the complete confirmed receiver receipt,
acknowledges the exact ID/digest in the source outbox, then confirms the source
acknowledgement before advancing. Cancellation arriving during a successful
source ACK still drains this last confirmation before rejecting.

Protocol, receiver identity, delivery identity, digest, byte length, confirmation
and decision counts must match. Invalid receipts never authorize source payload
reclamation. Receiver `omitted` decisions require `allowOmissions: true` on the
sender; the default stops instead of accepting partial application silently.
Allowing an already-retained omission acknowledges that original decision; it
does not ask the receiver to reconsider or overwrite conflicting data.

There is no persisted delivery cursor that can skip a failure. Each run starts
at the oldest pending row and stops on the first transport, conflict, validation,
acknowledgement or confirmation error. Retrying runs uses the existing outbox
identity/payload; it NEVER calls the original source callback. A lost receiver
response can mean the receiver committed: replay uses its retained receipt. A
lost source-ACK response can mean the source already reclaimed its payload:
the next run confirms source state before observing that outcome. Checkpoint
recovery itself remains the existing database/queue recovery API's responsibility;
a confirmation callback must reject until that outcome is known.

### Bounds, races and topology

`maxDeliveries` defaults to 100 and is capped at 10,000 selected entries per run,
including entries another sender acknowledges between selection and read.
`maxBytes` defaults to 64 MiB and is capped at 1 GiB of delivered payload bytes.
`maxMessageBytes` defaults to 8 MiB and is capped at 64 MiB. Metadata admission
checks happen before loading an oversized payload. A head that cannot fit a
fresh run rejects; it is never skipped to send smaller successors. Reaching
a run limit after progress returns `stopped: 'limit'` with later rows pending.
`stopped: 'empty'` describes the final pending read, not a promise that no future
writer will add work.

Only one run may be active on a pump instance; overlap rejects without a queue.
Independent pumps/connections can race, so the transport is at-least-once, not
exactly-once. Exact retained inbox receipts prevent repeated application SQL.
The pump handles a matching entry already acknowledged by a peer and refuses
missing/changed entries or nonadvancing metadata rather than spinning or skipping.
`deliveries`, `bytes`, `applied`, `omitted` and `replays` describe the confirmed
transfers in this run; applied/omitted include the original decisions of replays,
not only newly executed SQL. `alreadyAcknowledged` counts selection/read races.

**Use one fixed destination for an outbox's lifetime.** The existing outbox has
one acknowledgement bit, not per-recipient delivery state. This is not multicast,
fan-out, a work lease, consensus, source authentication, or a complete native
replication protocol. Changing a pump's recipient cannot recreate previously
acknowledged payloads. Do not manually acknowledge dependent entries out of order,
forget pending work, or modify the reserved tables. The application still owns
transport trust, destination identity, backup/retention policy and retry pacing.

The deadline passed to transport is the remaining run budget, not a fresh
per-message deadline. Cancellation never races away from an already-started
transport or storage operation. Source outbox calls use their existing bounded
SQL API and are checked before/after awaiting; a blocking adapter/transport can
therefore delay cancellation. Rejections retain the failing phase and delivery
identity, but are not blanket rollback claims or instructions to re-record work.

### Combined executed coverage

The combined receiver/pump suite passed **68/68 tests**, with no skipped tests,
on Node 22.16.0 / SQLite 3.49.1. Strict TypeScript 5.8.3 checks also passed.
The suite uses the actual capture, outbox, codec and applyChangeset modules with
real SQLite SQL, including ordered dependent updates, invalid receipts, dropped
responses, source/receiver confirmation failure, omission policy, bounds and
cancellation. Two file-backed senders overlap and recover using retained receipts.

Child processes are SIGKILLed after receiver commit before returning its ACK,
and after source acknowledgement before completing source confirmation. New
connections reopen both database files and recover without repeating source
callbacks or receiver inserts. A separate test writes and reopens real SQLite
images at the confirmation barriers and simulates losing a completed receiver
publication's response. Source payload bytes are retained until confirmation.
These are process-death and SQL-image tests on the reference engine, not physical
power-loss, browser OPFS/IndexedDB, Rust/WASM, or full-SDK certification.
