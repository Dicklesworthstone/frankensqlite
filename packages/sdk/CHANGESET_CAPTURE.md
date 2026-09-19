# Transaction-scoped outgoing changeset capture

`captureChangeset` captures ordinary callback SQL as a SQLite session-format
changeset. It complements `applyChangeset`: the application no longer has to
construct before/after rows by hand or copy and diff an entire database.

```ts
import { captureChangeset, applyChangeset } from '@frankensqlite/sdk';

// Explicit connection policy: required to observe REPLACE's implicit deletes.
await source.execute('PRAGMA recursive_triggers=ON');
const captured = await captureChangeset(source, async tx => {
  await tx.execute('UPDATE notes SET body=? WHERE id=?', ['revised', 12n]);
  await tx.execute('INSERT INTO tags(id, name) VALUES (?, ?)', [42n, 'reviewed']);
  return 'saved';
}, { tables: ['notes', 'tags'], maxRows: 10_000, maxBytes: 8 * 1024 * 1024 });

// Send only after capture resolves. The receiver still chooses its own policy.
await applyChangeset(destination, captured.changeset, { tables: ['notes', 'tags'] });
```

The helper owns one transaction through `ChangesetTarget`, implemented by
`FrankenDB` and `FrankenTransaction`. A transaction target supplies a child
savepoint. Await callback SQL; do not launch detached asynchronous work. A
successful result includes the callback `value`, owned `changeset` bytes, the
number of net `changes`, and `touchedRows` (distinct first-touch keys). A nested
result remains provisional until its enclosing transaction commits.

## Capture mechanism

Connection-local TEMP triggers retain the first state of each touched primary
key. INSERT records need only the key; UPDATE/DELETE record the original row.
At scope completion, bounded keyset reads traverse the journal and point lookups
read only the touched source rows. Repeated updates coalesce, insert-then-delete
vanishes, and delete-then-insert becomes an update. Primary-key changes produce
DELETE/INSERT pairs, with deletes ordered first within each table. Unchanged
non-key UPDATE fields are omitted, rather than encoded as SQL NULL.

The temporary journal and its counters participate in the same SQL rollback
and savepoints as application data. Output validation and cleanup happen before
commit. Callback errors, SQL constraints, cancelled/expired scopes, output-codec
limits, schema changes, and cleanup failures abort the owned transaction. No
persistent trigger, table snapshot, execute monkey-patch, native-session shim,
background task, or global writer lock is installed.

Storage classes survive round trips: integers travel as decimal text and become
signed int64 `bigint`; REAL remains `number`; text travels as typed bytes to
preserve embedded NUL and BOM, with UTF-8/UTF-16LE/UTF-16BE decoding. Blobs are
owned copies. Invalid text encodings fail explicitly. Byte/type comparisons are
independent of non-key column collations. Primary-key seeks use declared SQL
index semantics and then validate storage-class/BINARY key identity.

## Supported scope and deliberate boundaries

Capture requires 1..64 explicitly selected, existing ordinary tables in `main`,
with 1..256 visible, nongenerated columns and a declared primary key of 1..16
columns. Composite and WITHOUT ROWID primary keys are supported. Rows with any
NULL key component are outside session capture, including when key transitions
create or remove NULL-key rows. This is not whole-database replication.

`recursive_triggers` must already be ON; the helper never changes connection
policy. Captured tables must not have application triggers in either `main` or
`temp`: their execution order around generated INSERT keys cannot be inferred
safely from ordinary SQL triggers. Preflight rejects them before the callback.
Foreign-key actions and writes from triggers on other, uncaptured tables can be
observed in selected tables. The optional `indirect` boolean marks the entire
scope; this is not automatic native preupdate-depth attribution.

Receiver triggers and foreign-key actions retain their normal behavior. For
example, a receiver's cascading delete may remove a child before the captured
child DELETE is applied. The receiver must explicitly choose its NOTFOUND
conflict policy; capture neither disables constraints nor invents omissions.
Collated primary-key transitions can also conflict when inverting a changeset:
encoding an inverse is not a guarantee of constraint-free application.

Callbacks must not change schemas, transaction boundaries, capture TEMP objects,
or connection pragmas. Main and TEMP schema cookies and recursive-trigger policy
are checked before collection. Callbacks, SQL adapters, and database schema are
trusted; these checks are not a sandbox against deliberate journal/cookie
manipulation. Only one capture may be active per connection. Existing reserved
`__fsqlite_capture_*` TEMP objects are preserved and cause explicit rejection.

## Resource and durability boundaries

`maxRows` defaults to 10,000 (maximum 100,000). It bounds distinct touched keys,
including keys whose final change disappears. `maxBytes` defaults to 8 MiB
(maximum 64 MiB), and `maxCells` to 100,000 (maximum 1,000,000). They account for
journal images and subsequently collected source images, including record/slot
allowances and conservative text costs. SQL trigger guards enforce journal
admission. SQL length queries reject oversized postimages before transferring
values to JavaScript. Even a net-zero change can consume capture budget.

`limits` separately applies the existing changeset codec's byte/table/column/
change/cell bounds. Capture is bounded materialization, not streaming and not a
bound on arbitrary callback SQL execution, SQLite memory, process RSS, or disk
usage. `signal` and `timeoutMs` are cooperative and include collection/cleanup;
they cannot preempt arbitrary synchronous code inside a callback.

The returned bytes are not a durable outgoing queue. A process crash after SQL
commit but before the caller retains/sends them can lose the outgoing message.
Use transactional outbox persistence when delivery must survive that window.
Browser snapshot modes additionally require an explicit checkpoint; a memory SQL
commit alone is not a durable browser-storage acknowledgement.

## Verification

```sh
node --experimental-loader=./packages/sdk/tests/helpers/source-loader.mjs \
  --test packages/sdk/tests/changeset-capture.test.mjs
```

55 tests passed on Node 22.16.0 / SQLite 3.49.1 during implementation, including
20 deterministic native-session comparisons, binary application/inversion,
transaction/savepoint rollback, REPLACE, key changes, storage classes, UTF-16,
foreign-key actions, resource limits, cancellation, and indexed reads over a
10,000-row table. The NOCASE key-transition test proves forward application,
not equality with SQLite 3.49.1's native key-slot UPDATE representation or
unconditional inversion. These are real SQLite-reference tests of the SQL
helper, not certification of FrankenSQLite Rust/WASM or browser execution.

## Durable outgoing delivery with `ChangesetOutbox`

`ChangesetOutbox.record` closes the gap between source SQL commit and retaining
its outgoing message. It captures the callback and stores the resulting binary
payload, SHA-256, capture scope and stable delivery ID in the **same** owned SQL
transaction. Source changes cannot commit without the outbox record, and a
failed/rolled-back source transaction cannot leave a queued message.

```ts
import { ChangesetOutbox, applyChangeset } from '@frankensqlite/sdk';

await source.execute('PRAGMA recursive_triggers=ON');
const outbox = new ChangesetOutbox(source, {
  maxEntries: 10_000,
  maxPayloadBytes: 64 * 1024 * 1024,
});
const result = await outbox.record(async tx => {
  await tx.execute('UPDATE notes SET body=? WHERE id=?', ['revised', 12n]);
  return 'saved';
}, { deliveryId: 'source-42:operation-108', tables: ['notes'] });

// A transport can send this exact ID and payload after source durability.
const message = await outbox.read(result.delivery.deliveryId);
if (message !== null && message.changeset !== null) {
  await applyChangeset(destination, message.changeset, {
    deliveryId: message.delivery.deliveryId,
    tables: ['notes'],
  });
  // In snapshot mode, checkpoint destination BEFORE acknowledging the source.
  await outbox.acknowledge(message.delivery.deliveryId, message.delivery.sha256);
}
```

For browser snapshot persistence, checkpoint the source after `record` and
before treating its outgoing message as durably retained. Checkpoint the
receiver's rows/inbox before sending a durable ACK, and checkpoint the source's
acknowledgement state too. The helper does not perform those checkpoints or
pretend that a memory transaction is persistent storage.

### Recovery, ordering and ownership

Supply a stable, globally source-qualified `deliveryId` (valid UTF-8, 1..512
bytes, no NUL). A retained ID causes `record` to return `replayed: true` without
running the callback again. Fresh results have `replayed: false` and the callback
`value`; recovered results have no `value`, because arbitrary JavaScript return
values are not stored. Reusing an ID with a different table set or indirect flag
rejects. The helper cannot compare arbitrary callback bodies or business inputs:
never reuse an ID for a different operation, even if its capture scope matches.

An uncertain source commit acknowledgement is not permission to execute the SQL
again with a new ID. Retry the same ID: committed records are recovered, while
rolled-back work can run anew. Database effects and the outbox are atomic;
external callback side effects are not. A losing concurrent transaction may
have entered its callback before a storage conflict rolls it back. There is no
automatic replay, network transport, remote authentication or consensus here.

`pending({ limit, after })` returns frozen metadata in monotonic sequence order,
not payloads. Its default page is 100 entries, with a maximum of 256. `after` is
a nonnegative int64 `bigint`; AUTOINCREMENT prevents cursor reuse when old
acknowledged entries are forgotten. Deliver dependent changesets in sequence,
and do not advance a durable delivery cursor past unsuccessful entries. Multiple
senders may read the same pending entry; this is at-least-once delivery, not a
work-leasing protocol. Pair it with receiver-side `applyChangeset` receipts.

`read(id)` validates lengths, storage classes, the capture scope, codec, counts
and SHA-256 before returning one owned payload. An unknown ID returns null. An
acknowledged ID returns metadata with `changeset: null`. `pending` validates
metadata only; listing an entry is not a payload-integrity certificate.

`acknowledge(id, sha256)` must only be called after the receiver durably confirms
that exact ID and digest under the application's authentication/conflict policy.
Wrong/unknown acknowledgements reject. The first acknowledgement returns true;
repeating it returns false. Pending payload bytes are reclaimed, but the ID,
digest, original counts and capture scope remain to prevent callback replay.
An acknowledged receipt records delivery history, not the receiver's current
contents or an immutable guarantee about future database edits.

### Retention and schema contract

The outbox lives in reserved `main.__fsqlite_changeset_outbox`. Its schema,
primary-key sequence rule and BINARY delivery identity are validated; incompatible
existing tables, extra indexes and triggers are rejected rather than overwritten.
Metadata reads do not create the table. Source capture excludes SDK/system tables.
Application SQL and adapters are trusted, and must not edit outbox internals.
SHA-256 binds payload bytes to local metadata; it is not sender authentication or
protection against an authorized SQL writer rewriting both bytes and metadata.

`maxEntries` counts all retained IDs, including acknowledged ones (default 10,000,
maximum 100,000). `maxPayloadBytes` counts pending payload bytes (default 64 MiB,
maximum 1 GiB). Individual messages still obey capture/codec bounds. Capacity
checks abort the whole source transaction; acknowledged bytes free payload
capacity but do not free an ID slot. These are application accounting limits,
not total SQLite file-size or RSS bounds. Metadata/count scans are bounded by
the retained entry population; this is not an unbounded streaming log.

Only `forgetAcknowledged(id, sha256)` explicitly removes an exact acknowledged
identity. It refuses pending deliveries or a mismatched digest, and returns
false for an already absent ID. **Forgetting removes duplicate-operation
protection.** Retain IDs for the complete retry/redelivery horizon; never reuse
them for new work. Restoring an older source or receiver backup likewise restores
an older deduplication history. Nothing automatically expires or deletes IDs.

### Additional executed tests

The combined capture/outbox suite passed **90/90 tests**, with no skipped tests,
on Node 22.16.0 / SQLite 3.49.1. Outbox coverage includes native receiver apply,
duplicate work suppression, payload ownership/corruption, exact acknowledgement,
retention limits, schema checks, outer rollback, deferred commit failure, and
lost source/acknowledgement responses. Two file-backed connections are forced to
overlap at an absent delivery ID; only one source transaction commits.

Separate child processes reopen and deliver retained payloads. Additional child
processes are SIGKILLed after the SQL/outbox writes but before COMMIT, and after
COMMIT but before returning an acknowledgement. Reopen verifies the expected
atomic decision in each case. This proves those process-death cuts on the tested
SQLite platform, not power-loss resilience or FrankenSQLite Rust/WASM execution.

```sh
node --experimental-loader=./packages/sdk/tests/helpers/source-loader.mjs \
  --test packages/sdk/tests/changeset-capture.test.mjs \
  packages/sdk/tests/changeset-outbox.test.mjs
```
