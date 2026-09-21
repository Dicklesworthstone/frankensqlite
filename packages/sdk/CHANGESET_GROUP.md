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
Row order is deterministic first appearance, not SQLite's internal hash order.
Patchsets remain unsupported. Never replace the bytes of an already-identified
outbox delivery or treat composition as receiver acknowledgement.

## Executable checks

From the repository root, with workspace dependencies installed:

```sh
node --experimental-loader=./packages/sdk/tests/helpers/source-loader.mjs \
  --test packages/sdk/tests/changeset-group.test.mjs
```

The suite executes production TypeScript and native `node:sqlite` Session
capture/apply/inversion, including deterministic multi-session SQL histories.
The existing loader honors `FSQLITE_TYPESCRIPT_MODULE` when TypeScript is supplied
outside the workspace. It is not a WASM test or a Rust-workspace parity certificate.
