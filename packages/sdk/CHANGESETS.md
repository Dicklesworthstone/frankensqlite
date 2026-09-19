# SQLite session changesets in the JavaScript SDK

`decodeChangeset`, `encodeChangeset` and `invertChangeset` read and write the
SQLite session extension's binary changeset format. This is row-level change
interchange, not a whole database image, SQL dump, patchset, or new wire format.
It is independent of the native Rust session facade and does not pretend the
WASM handle has automatic session capture methods that it does not expose.

```ts
import { decodeChangeset, encodeChangeset, invertChangeset } from '@frankensqlite/sdk';
const tables = decodeChangeset(receivedBytes);
const wire = encodeChangeset(tables);
const undo = invertChangeset(wire);
```

The table descriptor carries its unqualified name, primary-key bytes in column
order, and changes in their original order. Integers decode as `bigint` (signed
64-bit), reals as `number`, text as `string`, blobs as owned `Uint8Array`, and SQL
NULL as `null`. `undefined` means an omitted UPDATE field, NOT SQL NULL. Author
integers using bigint, including `1n`; a number such as `1` intentionally encodes
a real. The indirect flag and nonzero primary-key ordinal bytes are preserved.
INSERT and DELETE carry complete rows; UPDATE keeps keys in its old record and
pairs old/new values only for modified non-key columns. Key changes must be a
DELETE and INSERT, as in SQLite's session output. Inversion keeps table/change
order and original UPDATE keys; it is not array reversal.

The decoder freezes structural arrays/objects. Blob bytes are independent,
exact-sized copies and remain mutable for normal JavaScript interoperability.
Changing a decoded blob changes a subsequent encoding, never the source bytes.
Table names are limited to 1024 UTF-8 bytes. Invalid UTF-8, unpaired UTF-16 on
encoding, NaN reals, NULL/missing keys, incomplete rows, malformed UPDATE pairs,
repeated table headers, noncanonical length varints, and truncated input reject.
Empty input is a valid empty changeset. Fixed non-shared ArrayBuffer-backed
inputs are required; shared, resizable and detached buffers are not accepted.

Optional limits cap bytes (default/hard maximum 64 MiB), tables (256), changes
(100,000), and total old/new slots (1,000,000), including omitted UPDATE slots.
Columns default to 2,000, configurable to 32,768 for custom SQLite builds.
Limits are checked before row allocation, not just after decoding. Encoding
uses the same semantic validator. These are protocol/allocation bounds, not
bounds on SQL execution memory or process RSS. Both operations materialize the
bounded changeset; neither is advertised as streaming.

Compact SQLite patchsets deliberately reject: they omit before-images and
cannot provide the same conflict detection or inversion guarantees. The codec
also does not implement changegroup coalescing, rebasing, automatic capture,
or authentication. Validate the source of received changes before applying
anything to application tables; format validation is not authorization.

## Transactional SQL application

`applyChangeset` connects the wire codec to the existing `FrankenDB` SQL path.
It also accepts a `FrankenTransaction`, using a recoverable child savepoint.

```ts
import { applyChangeset } from '@frankensqlite/sdk';

const result = await applyChangeset(db, receivedBytes, {
  tables: ['notes', 'tags'], // Required direct-target allowlist in main.
  signal: abortController.signal,
  timeoutMs: 10_000,
});
console.log(result.applied, result.omitted);
```

The complete wire input and allowlist are captured before asynchronous admission.
All target schemas are validated inside one owned transaction before any row is
written. Ordinary main tables with matching primary-key positions are supported,
including composite WITHOUT ROWID keys. Extra trailing target columns use their
defaults on INSERT and remain untouched by UPDATE. Missing/incompatible tables,
views, virtual/shadow tables, and generated/hidden columns fail explicitly.

DELETE checks every supplied non-key before-image; UPDATE checks only modified
fields. Matching retains the target column's SQLite affinity and collation, and
handles NULL separately from an omitted UPDATE field. Integral REALs are bound
as REAL, and 64-bit integers are never converted through JavaScript numbers.
SQL identifiers are quoted; values are bound, not interpolated.

By default, a missing row, changed before-image or duplicate primary key aborts
the complete application. An optional awaited `onConflict` callback receives
`kind` (`data`, `not-found`, or `conflict`), table, zero-based global change index,
column names and owned change images. It may return `omit` for that entry or
`abort`. Thrown errors, invalid callback results, cancellation and SQL errors
roll back the owned scope. SQL/constraint errors are never silently converted
to omissions. INSERT/UPDATE use OR ABORT to override schema IGNORE/REPLACE rules;
an ignored write cannot be counted as applied. Counts exclude trigger/FK effects.

This is deliberately not the entire native `sqlite3changeset_apply` API. Local
triggers and foreign-key actions retain ordinary SQL behavior, so the allowlist
is not a security sandbox or a promise that triggers cannot modify other tables.
Authenticate changesets and trust the target schema. The helper does not disable
constraints, synthesize FK deferral, implement REPLACE/constraint omission or
rebasing, automatically capture changes, or provide a replication transport.
For snapshot persistence, SQL commit still requires an explicit checkpoint to
publish durable browser storage; applying a changeset does not checkpoint.

## Duplicate delivery and lost acknowledgements

Supply a stable, source-qualified `deliveryId` to record a local inbox receipt
in the same transaction as the application rows:

```ts
const result = await applyChangeset(db, receivedBytes, {
  tables: ['notes', 'tags'],
  deliveryId: 'trusted-source-42:changeset-108',
});
if (result.replayed) {
  // This exact payload was already applied; no row SQL or resolver ran again.
}
```

The helper captures the bytes, hashes them with Web Crypto SHA-256, and stores
the identity, digest, byte length and applied/omitted counts in the reserved
`__fsqlite_changeset_receipts` table (exported as `CHANGESET_RECEIPTS_TABLE`).
IDs are case-sensitive, valid UTF-8, 1..512 bytes, without NUL. This option
requires Web Crypto; it hashes the bytes rather than accepting a caller digest.
The default path does not create an inbox or require Web Crypto.

A repeated ID with the same bytes returns the original counts and
`replayed: true`, without applying rows or invoking `onConflict` again. A new
application returns `replayed: false`. Reusing an ID for different bytes rejects
with `ERR_FSQLITE_CHANGESET_DELIVERY_REUSE`, even when byte lengths are equal.
Omissions are part of the original decision: changing a conflict policy on a
redelivery does not reconsider them. Authorization is still checked every time.

The receipt is an application-history record, not a claim that later SQL has
left the target rows unchanged. It remains usable after later schema changes.
Invalid receipt metadata or an incompatible inbox schema fails closed. The
inbox cannot be a direct changeset target; its primary key must use BINARY
collation, and inbox triggers/foreign keys are rejected. Application tables,
triggers, SQL adapters and the local database itself must still be trusted.
Hashes bind identities to bytes; they do not authenticate the sender or prevent
an authorized SQL writer from tampering with application data or receipts.

Rows and receipt commit or roll back together, including deferred commit errors
and a later rollback of an enclosing transaction. Concurrent conflicts are
surfaced through the existing transaction engine, not retried or serialized by
this helper. After an uncertain acknowledgement, resubmit the same ID and exact
bytes to the authoritative database: a retained receipt avoids repeating SQL.
This is not a guarantee about external callback side effects, replica consensus,
or durability beyond the target's own transaction/storage contract. In snapshot
modes, both rows and inbox still need the same explicit durable checkpoint.

Receipts are retained, not automatically expired or bounded in persistent row
count. Removing a receipt removes its duplicate-delivery protection. Retention
must follow the application's delivery/retry horizon; restoring an older backup
also restores an older inbox. Only a digest and fixed counters are retained per
ID, not the entire changeset. Hashing temporarily copies the already byte-bounded
input; this is not a streaming ingestion API or a process-RSS bound.

## Verification

```sh
node --experimental-loader=./packages/sdk/tests/helpers/source-loader.mjs \
  --test packages/sdk/tests/changeset-codec.test.mjs packages/sdk/tests/changeset-apply.test.mjs
```

Tests use Node's actual SQLite session extension to generate binary inputs,
compare exact re-encoding, and apply SDK-authored/inverted changesets. They
include composite WITHOUT ROWID keys, mixed operations, all storage classes,
integer extremes, UTF-8/NUL/BOM handling, malformed input and allocation limits,
and 30 deterministic native mutation workloads. This is SQLite wire-format
interoperability evidence, not FrankenSQLite engine/WASM/browser certification.

Application tests additionally compare native session application with ordinary
SQL for affinity/collation, mixed CRUD, inversion and 20 deterministic workloads.
They exercise atomic multi-table rollback, nested savepoints, explicit omissions,
constraint/trigger failures, schema preflight, quoted names, cancellation,
deadlines and wide before-images. These run the shipped TypeScript helper over
Node's SQLite SQL adapter; they do not claim a built FrankenSQLite WASM test.
Inbox tests cover duplicate delivery, same-ID payload substitution, lost commit
acknowledgements, rollback/constraint/cancellation cuts, retained omissions,
invalid schema/receipts, and a separate process reopening a real database file.
