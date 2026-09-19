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
