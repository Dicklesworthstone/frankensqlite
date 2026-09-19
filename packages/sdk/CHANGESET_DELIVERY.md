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
