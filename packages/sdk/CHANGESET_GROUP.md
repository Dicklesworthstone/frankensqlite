# Changeset composition

`concatChangesets(first, second, limits?)` combines consecutive SQLite Session
changesets into one reversible payload. This is useful when batching local edits
for offline replication, before assigning the resulting payload a delivery ID.
It uses the existing codec, so the result can be passed to `decodeChangeset`,
`invertChangeset`, or `applyChangeset` without another format or runtime.

```ts
import { concatChangesets, invertChangeset } from "@frankensqlite/sdk";

// first and second are Uint8Array changesets recorded in this order.
const batch = concatChangesets(first, second, {
  maxBytes: 4 * 1024 * 1024,
  maxChanges: 10_000,
});
const undo = invertChangeset(batch);
```

Composition follows `sqlite3changegroup_add`: inserts absorb later updates;
insert/delete pairs disappear; updates retain the earliest changed before-image
and latest after-image; update/delete pairs delete the original row; and
delete/insert pairs become updates or disappear when nothing changed. Restored
columns are omitted from updates. SQL NULL is distinct from an untouched field.
Ignored inconsistent-history transitions retain the original indirect flag;
actual merged changes are indirect only when both inputs were indirect.

Primary-key identity is storage-class and byte exact, as in SQLite Session, not
SQL affinity or application collation. Integers retain all 64 bits. Composite
keys, blobs, embedded NUL text and IEEE signed zero are not string-coerced into
one identity. Table names match ASCII-insensitively and keep their first spelling.
Column counts and every primary-key-position byte must agree; otherwise a
`ChangesetGroupError` with code `ERR_FSQLITE_CHANGESET_SCHEMA` is thrown.

Each input and the final encoded output independently obey `ChangesetLimits`.
Input blobs are copied by the decoder, inputs are never mutated, and output owns
its bytes. Scratch memory includes both decoded inputs, the key index, and the
output; these are payload/structure budgets, not a hard JavaScript heap or RSS
limit. Invalid input, incompatible schemas and budget overflow throw instead of
returning a partial payload. Empty changesets are valid; cancelled tables do not
leave invalid empty table headers.

This API does **not** rebase divergent histories, migrate schemas, preserve
intermediate trigger effects, implement native replication, or certify durable
storage. The caller is responsible for choosing consecutive compatible changes.
Table order follows first appearance. Row order is deterministic but is not a
byte-order compatibility guarantee with SQLite's internal hash order.
Patchsets remain unsupported. Never replace the bytes of an already-identified
outbox delivery or treat composition as receiver acknowledgement.

## Incremental groups

`ChangesetGroup` retains a primary-key index across additions, so callers can
combine many chunks without repeatedly decoding and serializing their entire
accumulated batch. `add()` is synchronous and touches only incoming rows and
their existing matches. Call `output()` when a payload is needed.

```ts
import { ChangesetGroup } from "@frankensqlite/sdk";

const group = new ChangesetGroup({
  maxBytes: 4 * 1024 * 1024,
  maxChanges: 10_000,
  maxTables: 32,
});

// chunks is an ordered iterable of consecutive Uint8Array changesets.
for (const chunk of chunks) group.add(chunk);
const payload = group.output();
const counters = group.stats();
// Hand payload to the application's durable delivery protocol before releasing
// its only retained copy. Neither output() nor clear() acknowledges delivery.
```

`add()` returns immutable counters and publishes only after the complete input,
all table layouts and the resulting retained-state limits have been validated.
On a format, schema or limit error, both output bytes and the previous counter
snapshot remain unchanged. The group remains usable; do not silently skip the
rejected chunk. A later deletion in the same input can free capacity for an
earlier insertion, provided the input itself and final retained state fit.

Counters contain `changes` (net rows), `cells` (old/new field slots), `tables`
(retained layouts), `byteLength` (exact current output size), and `schemaBytes`
(encoded header bytes for all retained layouts). Empty tables are omitted from
output but their layouts remain known: a table cannot silently change its column
count or primary key after all of its earlier edits cancel out. Those layouts
still count against `maxTables` and `maxBytes`, preventing unlimited schema-only
churn. Specifically, the retained byte charge is all schema headers plus net row
bytes, so it can be larger than `byteLength` when some tables have no net rows.

`clear()` explicitly releases all rows, schema history and counters while keeping
the configured limits. `output()` never consumes the group and returns a fresh
owned buffer each time. Caller mutations of input/output blobs or constructor
options cannot mutate retained state. Decoder bounds and fixed-buffer checks use
intrinsic typed-array metadata, ignoring shadowed accessors and methods; shared,
resizable, detached and proxied storage cannot bypass those checks.

## Executable checks

From the repository root, with workspace dependencies installed:

```sh
node --experimental-loader=./packages/sdk/tests/helpers/source-loader.mjs \
  --test packages/sdk/tests/changeset-codec.test.mjs \
  packages/sdk/tests/changeset-group.test.mjs
```

The same command is available as `npm run test:changeset-group` in `packages/sdk`.
The suite executes production TypeScript and native `node:sqlite` Session
capture/apply/inversion, including deterministic multi-session SQL histories.
The existing loader honors `FSQLITE_TYPESCRIPT_MODULE` when TypeScript is supplied
outside the workspace. It is not a WASM test or a Rust-workspace parity certificate.
